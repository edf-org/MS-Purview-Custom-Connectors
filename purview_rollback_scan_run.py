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


def _validate_purview_endpoint(url: str) -> str:
    """Validate a Purview endpoint against trusted Purview hosts."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https":
        raise ValueError("Purview endpoint must use https.")
    allowed = (
        host.endswith(".purview.azure.com")                    # classic accounts
        or host == "api.purview-service.microsoft.com"          # new unified portal
        or host.endswith("-api.purview-service.microsoft.com")  # tenant-specific / private endpoint form
    )
    if not allowed:
        raise ValueError(
            f"Purview endpoint host '{host}' is not in the trusted domain allow-list. "
            "Expected: *.purview.azure.com or *-api.purview-service.microsoft.com"
        )
    return url
 
 
def get_endpoint_and_token():
    """Resolve the Purview endpoint and acquire a bearer token."""
    from azure.identity import DefaultAzureCredential, AzureCliCredential
    from dotenv import load_dotenv
 
    load_dotenv()
    account = os.environ.get("PURVIEW_ACCOUNT_NAME")
    if not account:
        logger.error("PURVIEW_ACCOUNT_NAME is not set (env var or .env file).")
        sys.exit(1)

    # Validate account name format before constructing endpoint
    if not account.replace("-", "").replace("_", "").isalnum():
        logger.error(f"Invalid PURVIEW_ACCOUNT_NAME format: '{account}'")
        sys.exit(1)

    endpoint = f"https://{account}.purview.azure.com/datamap"
    # Validate the constructed endpoint
    endpoint = _validate_purview_endpoint(endpoint)

    # Credential selection. PURVIEW_USE_CLI_CREDENTIAL=true -> AzureCliCredential,
    # needed in environments like Azure Cloud Shell where DefaultAzureCredential
    # resolves to the Managed Identity, which may lack Data Curator on the
    # collection (its token is then rejected). Otherwise DefaultAzureCredential,
    # so production Managed Identity keeps working.
    use_cli = os.environ.get("PURVIEW_USE_CLI_CREDENTIAL", "false").strip().lower() == "true"
    if use_cli:
        logger.info("Acquiring Purview token via AzureCliCredential")
        credential = AzureCliCredential()
    else:
        logger.info("Acquiring Purview token via DefaultAzureCredential")
        credential = DefaultAzureCredential()
    token = credential.get_token(PURVIEW_SCOPE).token
    return endpoint, token
 
 
def _get_entity_scan_run_id(endpoint: str, token: str, guid: str):
    """Fetch one entity by GUID and return its scanRunId attribute, or None.

    Used to re-verify a keyword-search hit before it becomes eligible for
    deletion. Returns None if the entity can't be read or carries no scanRunId.

    Uses: GET {endpoint}/api/atlas/v2/entity/guid/{guid}?api-version=2023-09-01
    """
    import requests

    url = f"{endpoint}/api/atlas/v2/entity/guid/{guid}?api-version=2023-09-01"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        entity = resp.json().get("entity", {})
        return entity.get("attributes", {}).get("scanRunId")
    except Exception as exc:
        logger.warning(f"Could not fetch entity {guid} to verify scanRunId: {exc}")
        return None


def find_entities_by_run_id(endpoint: str, token: str, run_id: str) -> list:
    """Page through Purview search for all entities stamped with run_id.

    Uses keyword search: POST {endpoint}/api/search/query?api-version=2023-09-01
    with body {"keywords": run_id, "limit": ..., "offset": ...}. The target
    account rejects the Atlas attribute-filter form with a 400 ("attributeValue
    should not be null"); keyword search returns the matching entities under the
    response's "value" array.

    Keyword search is broader than an exact-attribute match, so every hit is
    re-verified: only entities whose scanRunId attribute actually equals run_id
    are returned — never one that merely mentions the ID incidentally. Search
    hits don't reliably carry custom attributes, so when a hit doesn't include
    scanRunId the entity is fetched by GUID to read it; any candidate whose
    scanRunId can't be confirmed equal to run_id is excluded (and logged) rather
    than risk deleting an unrelated entity.

    Returns a list of {"guid", "qualifiedName", "entityType"} dicts.
    """
    import requests

    url = f"{endpoint}/api/search/query?api-version=2023-09-01"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    candidates, offset = [], 0
    while True:
        body = {
            "keywords": run_id,
            "limit": SEARCH_PAGE_SIZE,
            "offset": offset,
        }
        resp = requests.post(url, json=body, headers=headers, timeout=60)
        resp.raise_for_status()
        page = resp.json().get("value", [])
        if not page:
            break
        for item in page:
            candidates.append({
                "guid": item.get("id"),
                "qualifiedName": item.get("qualifiedName"),
                "entityType": item.get("entityType"),
                # scanRunId may ride along on the search hit; if not, it's
                # resolved per-entity during verification below.
                "scanRunId": item.get("scanRunId"),
            })
        offset += len(page)
        if len(page) < SEARCH_PAGE_SIZE:
            break

    # Verify each hit against the exact scanRunId, since keyword search matches
    # the ID as free text across the whole entity, not just the scanRunId field.
    entities = []
    for c in candidates:
        scan_run_id = c["scanRunId"]
        if scan_run_id is None and c["guid"]:
            scan_run_id = _get_entity_scan_run_id(endpoint, token, c["guid"])
        if scan_run_id == run_id:
            entities.append({
                "guid": c["guid"],
                "qualifiedName": c["qualifiedName"],
                "entityType": c["entityType"],
            })
        else:
            logger.warning(
                f"Skipping search hit whose scanRunId != {run_id} "
                f"(scanRunId={scan_run_id!r}): "
                f"[{c.get('entityType', 'unknown')}] {c.get('qualifiedName', 'unknown')}"
            )

    return entities
 
 
def delete_entities(endpoint: str, token: str, entities: list) -> int:
    """Soft-delete entities in batches by GUID.
 
    Uses: DELETE {endpoint}/api/atlas/v2/entity/bulk?api-version=2023-09-01&guid=...&guid=...
    Returns the number of GUIDs submitted for deletion.
    """
    import requests
 
    headers = {"Authorization": f"Bearer {token}"}
    deleted = 0
    guids = []
    for e in entities:
        if e["guid"]:
            guids.append(e["guid"])
        else:
            # Log entities with missing GUIDs so the count reconciles with preview
            logger.warning(
                f"Entity missing GUID, skipping deletion: "
                f"[{e.get('entityType', 'unknown')}] {e.get('qualifiedName', 'unknown')}"
            )
 
    for i in range(0, len(guids), DELETE_BATCH_SIZE):
        batch = guids[i : i + DELETE_BATCH_SIZE]
        # entity/bulk 404s without api-version. Add it to params (not the URL
        # string) so requests builds the whole query string — api-version and
        # the repeated guid params are joined with correct ?/& automatically:
        #   .../entity/bulk?api-version=2023-09-01&guid=...&guid=...
        params = [("api-version", "2023-09-01")] + [("guid", g) for g in batch]
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
 
