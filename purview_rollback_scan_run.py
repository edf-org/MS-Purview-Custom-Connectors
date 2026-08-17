"""
Purview Scan Run Rollback Utility
==================================
 
Companion to the custom API connectors (Salesforce, Workday, NetSuite).
Finds every entity stamped with a given scanRunId and soft-deletes it.
 
WHEN TO USE
-----------
Only for the failure mode where a bad run CREATED entities that should not
exist. If a bad run merely wrote wrong values onto existing entities, do NOT
roll back — fix the connector and re-run; the upsert on qualifiedName
overwrites the bad values.
 
SAFETY MODEL
------------
- DRY RUN BY DEFAULT: lists what would be deleted; nothing is touched.
- Deletion in Purview is a SOFT delete: entities move to DELETED status and
  disappear from the catalog. A subsequent healthy scan recreates legitimate
  assets fresh via upsert.
- This is a standalone, manually-run script. Do NOT deploy it to the
  Function App or wire it to any trigger.
 
USAGE
-----
    # 1. See what a run wrote (safe, read-only):
    python purview_rollback_scan_run.py --run-id 20260716T031500Z-a3f9c2d1
 
    # 2. Actually delete it:
    python purview_rollback_scan_run.py --run-id 20260716T031500Z-a3f9c2d1 --execute
 
ENVIRONMENT
-----------
    PURVIEW_ACCOUNT_NAME   e.g. "purview-prod"
    Authentication uses DefaultAzureCredential:
      - locally: az login (your user needs Data Curator on the collection)
      - in Azure: Managed Identity (but again — run this manually, locally)
 
DEPENDENCIES
------------
    pip install azure-identity requests python-dotenv
"""
 
import argparse
import logging
import os
import sys
 
# Force UTF-8 on the log stream so Unicode (e.g. the "→" arrows in messages)
# renders correctly even on Windows consoles whose default codepage (cp1252)
# cannot encode it and would otherwise fall back to escapes like "→".
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
 
SEARCH_PAGE_SIZE = 50          # Purview search API page limit
DELETE_BATCH_SIZE = 20         # GUIDs per bulk-delete call (keep URLs short)
PURVIEW_SCOPE = "https://purview.azure.net/.default"
 
 
def get_endpoint_and_token():
    """Resolve the Purview endpoint and acquire a bearer token."""
    from azure.identity import DefaultAzureCredential
    from dotenv import load_dotenv
 
    load_dotenv()
    account = os.environ.get("PURVIEW_ACCOUNT_NAME")
    if not account:
        logger.error("PURVIEW_ACCOUNT_NAME is not set (env var or .env file).")
        sys.exit(1)
    endpoint = f"https://{account}.purview.azure.com"
    credential = DefaultAzureCredential()
    token = credential.get_token(PURVIEW_SCOPE).token
    return endpoint, token
 
 
def find_entities_by_run_id(endpoint: str, token: str, run_id: str) -> list:
    """Page through Purview search for all entities stamped with run_id.
 
    Uses: POST {endpoint}/datamap/api/search/query?api-version=2023-09-01
    Returns a list of {"guid", "qualifiedName", "entityType"} dicts.
    """
    import requests
 
    url = f"{endpoint}/datamap/api/search/query?api-version=2023-09-01"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
 
    entities, offset = [], 0
    while True:
        body = {
            "keywords": None,
            "limit": SEARCH_PAGE_SIZE,
            "offset": offset,
            "filter": {
                "attributeName": "scanRunId",
                "operator": "eq",
                "attributeValue": run_id,
            },
        }
        resp = requests.post(url, json=body, headers=headers, timeout=60)
        resp.raise_for_status()
        page = resp.json().get("value", [])
        if not page:
            break
        for item in page:
            entities.append({
                "guid": item.get("id"),
                "qualifiedName": item.get("qualifiedName"),
                "entityType": item.get("entityType"),
            })
        offset += len(page)
        if len(page) < SEARCH_PAGE_SIZE:
            break
 
    return entities
 
 
def delete_entities(endpoint: str, token: str, entities: list) -> int:
    """Soft-delete entities in batches by GUID.
 
    Uses: DELETE {endpoint}/api/atlas/v2/entity/bulk?guid=...&guid=...
    Returns the number of GUIDs submitted for deletion.
    """
    import requests
 
    headers = {"Authorization": f"Bearer {token}"}
    deleted = 0
    guids = [e["guid"] for e in entities if e["guid"]]
 
    for i in range(0, len(guids), DELETE_BATCH_SIZE):
        batch = guids[i : i + DELETE_BATCH_SIZE]
        params = [("guid", g) for g in batch]
        url = f"{endpoint}/api/atlas/v2/entity/bulk"
        resp = requests.delete(url, params=params, headers=headers, timeout=60)
        resp.raise_for_status()
        deleted += len(batch)
        logger.info(f"Deleted batch {i // DELETE_BATCH_SIZE + 1}: {len(batch)} entities")
 
    return deleted
 
 
def main():
    parser = argparse.ArgumentParser(description="Roll back a Purview connector scan run.")
    parser.add_argument("--run-id", required=True,
                        help="The scanRunId stamped on the bad run's entities "
                             "(from the connector's startup log / App Insights).")
    parser.add_argument("--execute", action="store_true",
                        help="Actually delete. Without this flag, dry run only.")
    args = parser.parse_args()
 
    endpoint, token = get_endpoint_and_token()
    logger.info(f"Searching {endpoint} for entities with scanRunId = {args.run_id}")
 
    entities = find_entities_by_run_id(endpoint, token, args.run_id)
    if not entities:
        logger.info("No entities found for that run ID. Nothing to do.")
        return
 
    logger.info(f"Found {len(entities)} entities from run {args.run_id}:")
    for e in entities:
        logger.info(f"  [{e['entityType']}] {e['qualifiedName']} ({e['guid']})")
 
    if not args.execute:
        logger.info("")
        logger.info(f"[DRY RUN] Would soft-delete {len(entities)} entities.")
        logger.info("[DRY RUN] Re-run with --execute to perform the deletion.")
        return
 
    confirm = input(f"Type the run ID ({args.run_id}) to confirm deletion: ").strip()
    if confirm != args.run_id:
        logger.info("Confirmation did not match. Aborting — nothing was deleted.")
        return
 
    count = delete_entities(endpoint, token, entities)
    logger.info(f"Done. Soft-deleted {count} entities from run {args.run_id}.")
    logger.info("Legitimate assets will be recreated by the next healthy scan (upsert).")
 
 
if __name__ == "__main__":
    main()
 
