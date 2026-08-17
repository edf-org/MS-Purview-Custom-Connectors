"""
Classification Name Verifier
============================
Checks that every classification name referenced in classification_rules.json
exists as a classification typedef in your Microsoft Purview account.

Microsoft no longer publishes the formal MICROSOFT.* typedef names in its docs,
so the only authoritative source is the Atlas API of your own tenant:

    GET {endpoint}/api/atlas/v2/types/typedefs?type=classification

Usage:
    # Live verification (requires env vars, same as the connectors):
    #   PURVIEW_ACCOUNT_NAME, and credentials for DefaultAzureCredential
    #   (AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET, or Managed Identity)
    python verify_classifications.py

    # Offline mode (no account configured): lists the distinct classification
    # names the rules file uses, so you can check them manually in Purview
    # Studio under Data Map > Classifications.

Exit codes: 0 = all names valid, 1 = missing names found, 2 = not configured / error.
"""
import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

RULES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "classification_rules.json")
PURVIEW_SCOPE = "https://purview.azure.net/.default"


def collect_rule_classifications(rules_path: str) -> dict:
    """Return {classification_name: {"enabled": bool, "sections": set}} for every rule."""
    with open(rules_path, "r", encoding="utf-8") as f:
        rules = json.load(f)

    used = {}
    for section in ("field_name_patterns", "field_type_rules", "object_field_rules"):
        for rule in rules.get(section, []):
            name = rule["classification"]
            entry = used.setdefault(name, {"enabled": False, "sections": set()})
            entry["sections"].add(section)
            if rule.get("enabled", True):
                entry["enabled"] = True
    return used


def fetch_defined_classifications(endpoint: str) -> set:
    """Fetch all classification typedef names from the Purview Atlas API."""
    import requests
    from azure.identity import DefaultAzureCredential

    credential = DefaultAzureCredential()
    token = credential.get_token(PURVIEW_SCOPE)
    resp = requests.get(
        f"{endpoint}/api/atlas/v2/types/typedefs",
        params={"type": "classification"},
        headers={"Authorization": f"Bearer {token.token}"},
        timeout=60,
    )
    resp.raise_for_status()
    defs = resp.json().get("classificationDefs", [])
    return {d["name"] for d in defs}


def suggest_alternatives(missing: str, defined: set, limit: int = 5) -> list:
    """Suggest defined typedefs that share the trailing token of a missing name
    (e.g. PHONE_NUMBER -> MICROSOFT.PERSONAL.US.PHONE_NUMBER)."""
    tail = missing.rsplit(".", 1)[-1]
    candidates = sorted(n for n in defined if tail in n)
    if not candidates and "_" in tail:
        last_word = tail.rsplit("_", 1)[-1]
        candidates = sorted(n for n in defined if last_word in n)
    return candidates[:limit]


def main() -> int:
    used = collect_rule_classifications(RULES_FILE)
    logger.info(f"Rules file references {len(used)} distinct classification names\n")

    account = os.environ.get("PURVIEW_ACCOUNT_NAME", "")
    if not account:
        logger.warning("PURVIEW_ACCOUNT_NAME not set — cannot verify against a live account.")
        logger.warning("Distinct classification names used by classification_rules.json:\n")
        for name in sorted(used):
            status = "enabled" if used[name]["enabled"] else "disabled"
            logger.warning(f"  [{status:8}] {name}")
        logger.warning("\nVerify these in Purview Studio (Data Map > Classifications) or set")
        logger.warning("PURVIEW_ACCOUNT_NAME + credentials and re-run for an automatic check.")
        return 2

    endpoint = f"https://{account}.purview.azure.com/datamap"
    logger.info(f"Fetching classification typedefs from {endpoint} ...")
    try:
        defined = fetch_defined_classifications(endpoint)
    except Exception as exc:
        logger.error(f"Failed to fetch typedefs: {exc}")
        return 2
    logger.info(f"Account defines {len(defined)} classification types\n")

    missing_enabled, missing_disabled = [], []
    for name in sorted(used):
        if name in defined:
            logger.info(f"  OK       {name}")
        elif used[name]["enabled"]:
            missing_enabled.append(name)
        else:
            missing_disabled.append(name)

    for name in missing_disabled:
        logger.warning(f"  MISSING  {name}  (rule disabled — must exist before enabling)")

    for name in missing_enabled:
        logger.error(f"  MISSING  {name}  (ENABLED rule — connector runs will fail to apply this)")
        for alt in suggest_alternatives(name, defined):
            logger.error(f"             did you mean: {alt}")

    logger.info("")
    if missing_enabled:
        logger.error(
            f"{len(missing_enabled)} enabled classification name(s) do not exist in this account. "
            "Fix the rules file or create custom classification types in Purview."
        )
        return 1
    logger.info("All enabled classification names exist in this Purview account.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
