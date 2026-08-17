"""
Microsoft Purview Custom Connector for Oracle NetSuite - Example Implementation
=================================================================================
 
This example demonstrates a complete custom connector that:
1. Authenticates to Oracle NetSuite via OAuth 1.0a (Token-Based Authentication)
2. Authenticates to Purview using Managed Identity / Service Principal
3. Discovers NetSuite record types and fields via the SuiteTalk REST Metadata Catalog
4. Registers custom type definitions in Purview for NetSuite ERP assets
5. Creates entities (account → record types → fields hierarchy) in Purview via Atlas v2
6. Builds cross-system lineage (NetSuite → ETL → Data Warehouse / BI)
7. Applies business metadata and classifications
 
This is the SAME approach used for Salesforce and Workday — the only difference
is which source API we call to discover metadata. The Purview side is identical.
 
 
PREREQUISITES
-------------
Before running this connector, you need the following infrastructure and accounts
in place. See the Architecture Document (Section 3) for full step-by-step setup.
 
1. Python 3.9+ installed on your development machine or deployed to Azure Functions.
   Download from https://www.python.org/downloads/ if not already installed.
   Verify with: python --version
 
2. Install the required Python packages. Run this in your terminal or command prompt
   from the same directory as this file:
 
       pip install pyapacheatlas azure-identity azure-keyvault-secrets requests requests-oauthlib python-dotenv
 
   Note: requests-oauthlib is required for NetSuite's OAuth 1.0a signature generation.
 
   If deploying to Azure Functions, add these to your requirements.txt file:
 
       azure-functions>=1.17.0
       pyapacheatlas>=0.14.0
       azure-identity>=1.15.0
       azure-keyvault-secrets>=4.8.0
       requests>=2.31.0
       requests-oauthlib>=2.0.0
       python-dotenv>=1.0.0
 
3. A Microsoft Purview account (Data Map enabled) in your Azure subscription.
   https://learn.microsoft.com/en-us/purview/create-microsoft-purview-portal
 
4. An Azure Key Vault to securely store NetSuite credentials.
   Azure portal > Create a resource > Key Vault > Create.
   https://learn.microsoft.com/en-us/azure/key-vault/general/quick-create-portal
 
5. An Oracle NetSuite account with SuiteTalk REST Web Services enabled.
   The connector reads metadata via the NetSuite REST API; it does not modify any
   NetSuite data. NetSuite is NOT a natively supported Purview data source.
 
6. A NetSuite Integration Record and Token-Based Authentication (TBA) configured.
   NetSuite's SuiteTalk REST API supports both OAuth 2.0 and OAuth 1.0a (TBA).
   TBA is the most commonly used approach for server-to-server integrations.
 
   Setup steps:
     a. In NetSuite, navigate to Setup > Company > Enable Features > SuiteCloud.
        Enable: "REST Web Services", "Token-Based Authentication".
     b. Create an Integration Record:
        Setup > Integration > Manage Integrations > New.
        - Name: "Purview Metadata Connector"
        - Check "Token-Based Authentication"
        - Save. Note the Consumer Key and Consumer Secret (shown only once).
     c. Create an Access Token:
        Setup > Users/Roles > Access Tokens > New.
        - Select the Integration record you just created.
        - Select a User and Role with API access (create a dedicated integration
          role with read-only access to the record types you need).
        - Save. Note the Token ID and Token Secret (shown only once).
     d. Create a dedicated Integration Role with least-privilege access:
        Setup > Users/Roles > Manage Roles > New.
        - Name: "Purview API Read-Only"
        - Under Permissions > Transactions/Lists/Reports, grant View access
          to the record types you need to catalog.
        - Grant "REST Web Services" permission under Setup.
     e. Note your NetSuite Account ID:
        Setup > Company > Company Information > Account ID.
        Format: e.g., "1234567" or "1234567_SB1" for sandbox.
 
   FOR OAUTH 2.0 (alternative — newer, but requires authorization code flow):
     a. Enable OAuth 2.0 in SuiteCloud features.
     b. Create an Integration Record with OAuth 2.0 scope.
     c. Complete the authorization code flow to obtain tokens.
     d. OAuth 2.0 is preferred for SuiteProjects Pro REST API but TBA is
        still the standard for SuiteTalk REST Web Services.
 
7. Authentication to Purview — choose one of:
 
   a. Managed Identity (recommended for Azure-hosted deployments):
      - Enable System-Assigned Managed Identity on your Azure Function App.
      - Assign the identity "Data Curator" and "Data Source Administrator" roles
        in the Purview governance portal (Data Map > Collections > Role assignments).
      - Grant the identity "Key Vault Secrets User" role on your Azure Key Vault.
 
   b. Service Principal (required for local development or non-Azure environments):
      - Register an App in Microsoft Entra ID (Azure portal > App registrations).
      - Create a client secret and assign Purview roles.
      - Set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET env vars.
 
8. Where to run this connector:
 
   LOCAL DEVELOPMENT:
     - Run directly on your laptop/workstation with Python installed.
     - Use a .env file for environment variables (see below).
     - Uses Service Principal authentication.
 
   AZURE FUNCTIONS (recommended for production):
     - Deploy as a Timer Trigger (scheduled) or HTTP Trigger (on-demand).
     - Uses Managed Identity for Purview and Key Vault — no secrets in code.
     - Project structure:
         purview-netsuite-connector/
         ├── function_app.py
         ├── host.json
         ├── local.settings.json          # DO NOT commit
         ├── requirements.txt
         └── purview_netsuite_connector_example.py
 
   AZURE CONTAINER APPS / ON-PREMISES SERVER:
     - Same approach; use Service Principal if not running in Azure.
 
 
ENVIRONMENT VARIABLES
---------------------
Required for ALL environments:
    PURVIEW_ACCOUNT_NAME   Your Purview account name, WITHOUT the domain suffix.
                           Example: "my-purview-account"
                           Where to find it: Azure portal > your Purview resource > Overview.
 
    KEY_VAULT_URL          The full URL of your Azure Key Vault.
                           Example: "https://my-keyvault.vault.azure.net/"
 
Required ONLY for Service Principal authentication (local dev / non-Azure):
    AZURE_TENANT_ID        Your Microsoft Entra ID tenant ID.
    AZURE_CLIENT_ID        The Application (client) ID of your registered app.
    AZURE_CLIENT_SECRET    The client secret value for your registered app.
 
NOT required when using Managed Identity in Azure.
 
Example .env file:
    # .env — DO NOT COMMIT TO SOURCE CONTROL
    AZURE_TENANT_ID=12a345bc-67d1-ef89-abcd-efg12345abcde
    AZURE_CLIENT_ID=a1234bcd-5678-9012-abcd-abcd1234abcd
    AZURE_CLIENT_SECRET=xYz...your-secret-value...
    PURVIEW_ACCOUNT_NAME=my-purview-account
    KEY_VAULT_URL=https://my-keyvault.vault.azure.net/
 
 
AZURE KEY VAULT SECRETS REQUIRED
---------------------------------
Create these secrets in your Azure Key Vault BEFORE running the connector.
Azure portal > Key Vault > Objects > Secrets > + Generate/Import.
 
    Secret Name                     Description                                      Example Value
    ----------------------------    -----------------------------------------------  ------------------------------------
    netsuite-account-id             Your NetSuite Account ID. Found under            1234567
                                    Setup > Company > Company Information.
                                    For sandboxes, append _SB1 (e.g., 1234567_SB1).
 
    netsuite-consumer-key           Consumer Key from the Integration Record.        a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4...
                                    Found under Manage Integrations > your record.
                                    IMPORTANT: Only shown once at creation time.
 
    netsuite-consumer-secret        Consumer Secret from the Integration Record.     f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3...
                                    IMPORTANT: Only shown once at creation time.
 
    netsuite-token-id               Token ID from the Access Token.                  a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4...
                                    Found under Setup > Users/Roles > Access Tokens.
                                    IMPORTANT: Only shown once at creation time.
 
    netsuite-token-secret           Token Secret from the Access Token.              f6e5d4c3b2a1f6e5d4c3b2a1f6e5d4c3...
                                    IMPORTANT: Only shown once at creation time.
 
Granting the connector access to read these secrets:
    - Managed Identity: Key Vault > Access control > "Key Vault Secrets User" role
      for your Azure Function App.
    - Service Principal: Same role for your App Registration.
 
 
USAGE
-----
1. DRY-RUN MODE (default, no credentials needed):
       python purview_netsuite_connector_example.py
 
2. LIVE MODE:
   a. Set up all prerequisites. b. Create Key Vault secrets. c. Set env vars.
   d. Uncomment all "--- Uncomment for real usage ---" blocks.
   e. Run: python purview_netsuite_connector_example.py
 
3. AZURE FUNCTIONS DEPLOYMENT:
   Create function_app.py:
 
       import azure.functions as func
       from purview_netsuite_connector_example import main as run_connector
 
       app = func.FunctionApp()
 
       @app.timer_trigger(schedule="0 0 3 * * *", arg_name="timer")
       def purview_netsuite_scan(timer: func.TimerRequest) -> None:
           run_connector()
 
   Deploy: func azure functionapp publish <your-function-app-name>
 
4. CUSTOMIZATION:
   - Edit RECORD_TYPES_TO_SCAN to change which NetSuite records are cataloged.
   - Edit LINEAGE_MAPPINGS to change cross-system lineage definitions.
"""
 
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Optional
 
# Shared, configuration-driven classification engine (rules live in
# classification_rules.json — maintained by data stewards)
from classification_engine import ClassificationEngine
 
# --- Uncomment these imports when running against real Purview + NetSuite ---
# from pyapacheatlas.auth import ServicePrincipalAuthentication
# from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess
# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
# from requests_oauthlib import OAuth1
# from dotenv import load_dotenv
# import requests
 
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
 
 
# =============================================================================
# SECURITY HARDENING CONTROLS (see Purview_Connector_Security_Review.md)
# Ported from the SQL reference connector: M1 sanitization, M2 retry,
# M3 business-metadata gating, L1 endpoint allow-list, LLM10 breadth caps,
# LLM04 metadata drift detection.
# =============================================================================
 
# --- Security: default timeout for all HTTP requests (seconds) ---
# Tuple (connect_timeout, read_timeout); applied by _request_with_retry.
REQUEST_TIMEOUT = (10, 30)

# Dry-run transport toggle. When true (default), HTTP calls are simulated by
# _DryRunResponse *through the same request path* used in live mode — so
# identifier validation, the timeout, the OAuth1 auth object, and the
# retry/backoff wrapper are all genuinely exercised without real credentials.
# Live mode: set CONNECTOR_DRY_RUN=false and uncomment the requests import.
DRY_RUN = os.environ.get("CONNECTOR_DRY_RUN", "true").strip().lower() != "false"


def _validate_identifier(value: str, allow_list: list = None) -> str:
    """Validate that a string is a safe SuiteQL/API identifier.

    Rejects characters usable for SuiteQL/path injection (spaces, quotes,
    semicolons, dots, slashes, etc.). Optionally checks an allow-list. Raises
    ValueError if unsafe.
    """
    if allow_list and value not in allow_list:
        raise ValueError(f"Identifier '{value}' is not in the allow-list.")
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', value):
        raise ValueError(f"Identifier '{value}' contains invalid characters.")
    return value


# SuiteQL allow-list: maps public record_type names to their SuiteQL table
# expressions. Used by get_record_count() (the table expression is pre-approved,
# never built from raw record_type) and as the discovery allow-list. Add entries
# here when adding record types to RECORD_TYPES_TO_SCAN.
SUITEQL_TABLE_MAP = {
    "customer":      "customer",
    "vendor":        "vendor",
    "employee":      "employee",
    "salesOrder":    "transaction WHERE type = 'SalesOrd'",
    "invoice":       "transaction WHERE type = 'CustInvc'",
    "purchaseOrder": "transaction WHERE type = 'PurchOrd'",
    "vendorBill":    "transaction WHERE type = 'VendBill'",
    "inventoryItem": "item WHERE itemType = 'InvtPart'",
    "journalEntry":  "transaction WHERE type = 'Journal'",
    "account":       "account",
}
 
 
# --- M1: metadata sanitization (see Security Review) ---
# Source-system metadata (field names, labels, descriptions) is UNTRUSTED
# input: it flows into the Purview catalog, which may later be consumed by
# AI/Copilot experiences and RAG pipelines. Sanitizing here closes both the
# classic injection surface (qualifiedName manipulation, log forging) and
# reduces the stored prompt-injection / catalog-poisoning surface
# (OWASP GenAI LLM01 / LLM04).
MAX_NAME_COMPONENT_LENGTH = 256
MAX_DESCRIPTION_LENGTH = 2000
 
 
def _safe_name_component(value) -> str:
    """Validate a source-derived name component before qualifiedName use.
 
    qualifiedNames form the catalog hierarchy. A field/object name containing
    '/', whitespace, or control characters could corrupt the hierarchy or
    spoof another asset's identity. Raises ValueError so malicious or corrupt
    source metadata fails loudly instead of silently polluting the Data Map.
    """
    value = str(value or "")
    if not re.match(r'^[A-Za-z0-9_.\-]{1,%d}$' % MAX_NAME_COMPONENT_LENGTH, value):
        raise ValueError(f"Unsafe qualifiedName component from source system: {value[:64]!r}")
    return value
 
 
def _sanitize_text(value, max_length: int = MAX_DESCRIPTION_LENGTH) -> str:
    """Sanitize free-text metadata (labels, descriptions) from source systems.
 
    Strips control characters (log forging, rendering exploits), collapses
    whitespace/newlines, and caps length. It deliberately does NOT attempt to
    detect 'prompt injection' phrasing (pattern-matching is unreliable);
    downstream AI consumers should treat catalog descriptions as untrusted
    data regardless. Provenance tagging (sourceOfTruth attribute) lets those
    consumers distinguish connector-written text from curated text.
    """
    if not value:
        return ""
    value = re.sub(r'[\x00-\x1f\x7f-\x9f]', ' ', str(value))
    value = re.sub(r'\s+', ' ', value).strip()
    return value[:max_length]
 
 
# --- M2: retry with exponential backoff (see Security Review) ---
MAX_RETRIES = 4
 
 
class _DryRunResponse:
    """Minimal stand-in for requests.Response used in dry-run mode.

    Lets the full request path — OAuth1 auth, the configured timeout, and the
    retry/backoff wrapper — execute unchanged without a live endpoint or the
    `requests`/`requests-oauthlib` dependencies. status_code is 200 so the
    success path runs; json() returns the simulated payload.
    """

    def __init__(self, payload, url: str, method: str, timeout):
        self._payload = payload if payload is not None else {}
        self.url = url
        self.request_method = method
        self.request_timeout = timeout
        self.status_code = 200
        self.headers = {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


def _request_with_retry(method: str, url: str, dry_run_payload=None, **kwargs):
    """HTTP request with exponential backoff, for BOTH live and dry-run.
 
    Retries on 429 (honoring Retry-After — important for shared source-API
    quotas like Salesforce daily limits), transient 5xx, and connection or
    timeout errors. Requires the `requests` import to be uncommented.
 
    Usage in live mode (replaces bare requests.get/post calls):
        response = _request_with_retry("GET", url, headers=headers)
        response = _request_with_retry("POST", url, json=payload, headers=headers)
    """
    import time
    import random
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)
    for attempt in range(MAX_RETRIES + 1):
        try:
            if DRY_RUN:
                # Simulated transport: exercises the OAuth1 auth, timeout, and
                # this retry/backoff wrapper without a live endpoint.
                logger.info(
                    f"[DRY RUN] {method} {url.split('?')[0]} (timeout={kwargs['timeout']})"
                )
                payload = dry_run_payload() if callable(dry_run_payload) else dry_run_payload
                response = _DryRunResponse(payload, url, method, kwargs["timeout"])
            else:
                response = requests.request(method, url, **kwargs)  # noqa: F821 (live mode)
            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt == MAX_RETRIES:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else (2 ** attempt) + random.random()
                logger.warning(
                    f"HTTP {response.status_code} from {url.split('?')[0]}; "
                    f"retry {attempt + 1}/{MAX_RETRIES} in {delay:.1f}s"
                )
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response
        except Exception as exc:
            if exc.__class__.__name__ in ("ConnectionError", "Timeout", "ChunkedEncodingError"):
                if attempt == MAX_RETRIES:
                    raise
                delay = (2 ** attempt) + random.random()
                logger.warning(
                    f"{exc.__class__.__name__} calling {url.split('?')[0]}; "
                    f"retry {attempt + 1}/{MAX_RETRIES} in {delay:.1f}s"
                )
                time.sleep(delay)
                continue
            raise
 
 
# --- M3: business metadata gating (see Security Review) ---
# The Step 6 example values in this file (quality scores, owners, validation
# dates) are PLACEHOLDERS. Applying fabricated governance signals to a
# production catalog misleads both humans and AI features that treat the
# catalog as authoritative (OWASP GenAI LLM09 Misinformation). This flag
# defaults to OFF; set APPLY_BUSINESS_METADATA=true only after replacing the
# placeholders with values computed from real sources (data-quality jobs,
# ownership registries, etc.). PII/financial CLASSIFICATIONS are NOT gated:
# they derive from actual field discovery and are safe to apply.
APPLY_BUSINESS_METADATA = os.environ.get("APPLY_BUSINESS_METADATA", "false").strip().lower() == "true"
 
# --- L1: Purview endpoint allow-list (see Security Review) ---
# The PURVIEW_ENDPOINT override supports the new unified portal, but an
# unvalidated override would let anyone able to tamper with the runtime
# environment (App Settings, container env) redirect the full metadata
# stream — including PII classifications and record counts — to an
# attacker-controlled endpoint. Only recognized Microsoft Purview hosts
# are accepted.
 
def _validate_purview_endpoint(url: str) -> str:
    """Validate a PURVIEW_ENDPOINT override against trusted Purview hosts."""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.scheme != "https":
        raise ValueError("PURVIEW_ENDPOINT must use https.")
    allowed = (
        host.endswith(".purview.azure.com")                    # classic accounts
        or host == "api.purview-service.microsoft.com"          # new unified portal
        or host.endswith("-api.purview-service.microsoft.com")  # tenant-specific / private endpoint form
    )
    if not allowed:
        raise ValueError(
            f"PURVIEW_ENDPOINT host '{host}' is not a recognized Microsoft Purview "
            f"endpoint; refusing to send metadata to an untrusted destination."
        )
    return url
 
 
# --- LLM10 hardening: discovery breadth caps (see Security Review §2.8) ---
# Unbounded discovery (scan-all mode, no per-object field caps) can exhaust
# shared source-API quotas — e.g., Salesforce daily request limits are an
# org-wide resource, so exhausting them is a denial-of-service against every
# other integration in the org. Caps are env-configurable; objects/fields
# beyond the cap are logged and skipped, never silently dropped.
MAX_OBJECTS_PER_RUN = int(os.environ.get("MAX_OBJECTS_PER_RUN", "200"))
MAX_FIELDS_PER_OBJECT = int(os.environ.get("MAX_FIELDS_PER_OBJECT", "500"))
 
 
def _apply_cap(items, cap: int, what: str):
    """Truncate a discovery list to a cap, logging what was skipped."""
    items = list(items)
    if len(items) > cap:
        logger.warning(
            f"Discovery cap: {len(items)} {what} found, processing first {cap} "
            f"(raise the cap via env var if intentional); skipping {len(items) - cap}"
        )
        return items[:cap]
    return items
 
 
# --- LLM04 hardening: metadata drift detection (see Security Review §3) ---
# Poisoning the Purview catalog is poisoning the grounding source for every
# AI feature built on it. This optional check compares the descriptions the
# connector is about to ingest against the previous run's state and warns on
# any changed or removed asset descriptions — a review signal for curators
# before drifted/poisoned metadata is treated as certified.
# Enable by setting DRIFT_STATE_PATH to a writable JSON file path (local disk
# for testing; an Azure Files mount or downloaded/uploaded Blob in production).
 
def _check_metadata_drift(entries, state_path: str = None) -> dict:
    """Compare (qualifiedName → description hash) against the previous run.
 
    Args:
        entries: iterable of (qualified_name, description) tuples.
        state_path: JSON state file path; defaults to the DRIFT_STATE_PATH
            environment variable. If neither is set, the check is disabled.
 
    Returns:
        {"changed": [...], "added": [...], "removed": [...]} (empty if disabled).
    """
    import hashlib
    import json as _json
    state_path = state_path or os.environ.get("DRIFT_STATE_PATH", "")
    result = {"changed": [], "added": [], "removed": []}
    if not state_path:
        return result
 
    current = {qn: hashlib.sha256((desc or "").encode()).hexdigest()
               for qn, desc in entries}
    previous = {}
    if os.path.exists(state_path):
        try:
            with open(state_path) as fh:
                previous = _json.load(fh)
        except Exception as exc:
            logger.warning(f"Drift state unreadable ({exc.__class__.__name__}); treating all assets as new")
 
    for qn, digest in current.items():
        if qn not in previous:
            result["added"].append(qn)
        elif previous[qn] != digest:
            result["changed"].append(qn)
    result["removed"] = [qn for qn in previous if qn not in current]
 
    if result["changed"]:
        logger.warning(
            f"METADATA DRIFT: {len(result['changed'])} asset description(s) changed since the "
            f"last run — review before treating as certified: {result['changed'][:10]}"
        )
    if result["removed"]:
        logger.warning(
            f"METADATA DRIFT: {len(result['removed'])} previously seen asset(s) no longer "
            f"present: {result['removed'][:10]}"
        )
    if result["added"] and previous:
        logger.info(f"Metadata drift: {len(result['added'])} new asset(s) since the last run")
 
    try:
        with open(state_path, "w") as fh:
            _json.dump(current, fh)
    except Exception as exc:
        logger.warning(f"Could not persist drift state ({exc.__class__.__name__})")
    return result
 
 
 
# =============================================================================
# 0. SCAN RUN IDENTIFIER (rollback support)
# =============================================================================
# Every entity written by this run is stamped with this ID via the custom
# "scanRunId" attribute. If a run goes bad, the standalone utility
# purview_rollback_scan_run.py can locate everything stamped with a given
# run ID and soft-delete it. The ID is logged at startup so it is captured
# in Application Insights — always note it before/after a production run.
 
SCAN_RUN_ID = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
logger.info(f"Scan run ID: {SCAN_RUN_ID}")
 
# Provenance tagging (Security Review 8.2): every connector-written entity is
# marked so downstream consumers — human curators and AI features alike — can
# distinguish machine-synced text from human-curated text.
SOURCE_OF_TRUTH = "connector:netsuite"
 
 
# =============================================================================
# 1. CONFIGURATION
# =============================================================================
 
# NetSuite record types to catalog in Purview.
# These correspond to SuiteTalk REST API record endpoints:
#   GET /services/rest/record/v1/{recordType}
# The metadata catalog is at:
#   GET /services/rest/record/v1/metadata-catalog/{recordType}
RECORD_TYPES_TO_SCAN = [
    {"record_type": "customer", "display_name": "Customer", "category": "Lists",
     "description": "Customer records (companies, individuals, leads, prospects)"},
    {"record_type": "vendor", "display_name": "Vendor", "category": "Lists",
     "description": "Vendor/supplier records"},
    {"record_type": "employee", "display_name": "Employee", "category": "Lists",
     "description": "Employee records"},
    {"record_type": "salesOrder", "display_name": "Sales Order", "category": "Transactions",
     "description": "Sales order transaction records"},
    {"record_type": "invoice", "display_name": "Invoice", "category": "Transactions",
     "description": "Invoice transaction records"},
    {"record_type": "purchaseOrder", "display_name": "Purchase Order", "category": "Transactions",
     "description": "Purchase order transaction records"},
    {"record_type": "vendorBill", "display_name": "Vendor Bill", "category": "Transactions",
     "description": "Vendor bill (AP invoice) records"},
    {"record_type": "inventoryItem", "display_name": "Inventory Item", "category": "Items",
     "description": "Inventory and stock item records"},
    {"record_type": "journalEntry", "display_name": "Journal Entry", "category": "Transactions",
     "description": "General ledger journal entries"},
    {"record_type": "account", "display_name": "Account (GL)", "category": "Lists",
     "description": "Chart of accounts / general ledger account records"},
]
 
# Cross-system lineage mappings
LINEAGE_MAPPINGS = [
    {
        "source_records": ["customer", "salesOrder", "invoice"],
        "process_name": "Revenue Data Sync",
        "process_type": "ETL",
        "destination_table": "dwh://analytics-warehouse/finance/fact_revenue",
        "destination_type": "custom_sql_table",
        "description": "Daily sync of NetSuite customer, order, and invoice data to the revenue fact table",
    },
    {
        "source_records": ["vendor", "purchaseOrder", "vendorBill"],
        "process_name": "AP Data Sync",
        "process_type": "ETL",
        "destination_table": "dwh://analytics-warehouse/finance/fact_accounts_payable",
        "destination_type": "custom_sql_table",
        "description": "Daily sync of NetSuite vendor and purchasing data to the AP fact table",
    },
    {
        "source_records": ["inventoryItem"],
        "process_name": "Inventory Sync",
        "process_type": "ETL",
        "destination_table": "dwh://analytics-warehouse/supply_chain/dim_product",
        "destination_type": "custom_sql_table",
        "description": "Daily sync of NetSuite inventory items to the product dimension table",
    },
    {
        "source_records": ["account", "journalEntry"],
        "process_name": "GL Data Sync",
        "process_type": "ETL",
        "destination_table": "dwh://analytics-warehouse/finance/fact_general_ledger",
        "destination_type": "custom_sql_table",
        "description": "Daily sync of NetSuite chart of accounts and journal entries to the GL fact table",
    },
]
 
 
@dataclass
class PurviewConfig:
    """Configuration for connecting to Microsoft Purview."""
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    account_name: str = ""
 
    @classmethod
    def from_environment(cls) -> "PurviewConfig":
        # load_dotenv()
        return cls(
            tenant_id=os.environ.get("AZURE_TENANT_ID", ""),
            client_id=os.environ.get("AZURE_CLIENT_ID", ""),
            client_secret=os.environ.get("AZURE_CLIENT_SECRET", ""),
            account_name=os.environ.get("PURVIEW_ACCOUNT_NAME", ""),
        )
 
    @property
    def endpoint(self) -> str:
        # L1: PURVIEW_ENDPOINT override is allow-list validated so a tampered
        # App Setting cannot redirect the metadata stream.
        override = os.environ.get("PURVIEW_ENDPOINT", "").rstrip("/")
        if override:
            return _validate_purview_endpoint(override)  # L1: allow-list check
        return f"https://{self.account_name}.purview.azure.com/datamap"
 
 
@dataclass
class NetSuiteConfig:
    """Configuration for connecting to Oracle NetSuite."""
    account_id: str = ""          # e.g., "1234567" or "1234567_SB1"
    consumer_key: str = ""
    consumer_secret: str = ""
    token_id: str = ""
    token_secret: str = ""
 
    @classmethod
    def from_key_vault(cls, kv_url: str) -> "NetSuiteConfig":
        """Load NetSuite credentials from Azure Key Vault."""
        # --- Uncomment for real usage ---
        # credential = DefaultAzureCredential()
        # kv_client = SecretClient(vault_url=kv_url, credential=credential)
        # return cls(
        #     account_id=kv_client.get_secret("netsuite-account-id").value,
        #     consumer_key=kv_client.get_secret("netsuite-consumer-key").value,
        #     consumer_secret=kv_client.get_secret("netsuite-consumer-secret").value,
        #     token_id=kv_client.get_secret("netsuite-token-id").value,
        #     token_secret=kv_client.get_secret("netsuite-token-secret").value,
        # )
        logger.info(f"[DRY RUN] Would retrieve NetSuite credentials from Key Vault: {kv_url}")
        return cls(
            account_id="1234567",
            consumer_key="dry-run-consumer-key",
            consumer_secret="dry-run-consumer-secret",
            token_id="dry-run-token-id",
            token_secret="dry-run-token-secret",
        )
 
    @property
    def base_url(self) -> str:
        """Base URL for NetSuite SuiteTalk REST API."""
        # SSRF/subdomain-injection guard: account_id becomes the host subdomain,
        # so it must be strictly alphanumeric+underscore. The .suitetalk.api.
        # netsuite.com suffix is hardcoded (never Key-Vault-supplied), so a full
        # _validate_url_domain check does not apply here. Account IDs may start
        # with a digit (e.g. "1234567"), so _validate_identifier is not usable.
        if not re.match(r'^[A-Za-z0-9_]+$', self.account_id):
            raise ValueError(
                f"NetSuite account_id '{self.account_id}' contains invalid characters. "
                f"Expected alphanumerics and underscores only."
            )
        account_slug = self.account_id.lower().replace("_", "-")
        return f"https://{account_slug}.suitetalk.api.netsuite.com/services/rest"
 
    @property
    def record_api_url(self) -> str:
        """URL for the Record API (CRUD + metadata)."""
        return f"{self.base_url}/record/v1"
 
    @property
    def suiteql_url(self) -> str:
        """URL for the SuiteQL query API."""
        return f"{self.base_url}/query/v1/suiteql"
 
 
# =============================================================================
# 2. AUTHENTICATION SERVICES
# =============================================================================
 
class PurviewAuthService:
    """Handles authentication to Microsoft Purview."""
 
    def __init__(self, config: PurviewConfig):
        self.config = config
 
    def get_bearer_token(self) -> str:
        # --- Uncomment for real usage ---
        # credential = DefaultAzureCredential()
        # token = credential.get_token("https://purview.azure.net/.default")
        # return token.token
        logger.info("[DRY RUN] Would acquire Purview bearer token via DefaultAzureCredential")
        return "dry-run-purview-token"
 
 
class NetSuiteAuthService:
    """Handles OAuth 1.0a Token-Based Authentication to Oracle NetSuite.
 
    NetSuite uses OAuth 1.0a with HMAC-SHA256 signature method.
    Every request is signed using four credentials:
    - Consumer Key + Consumer Secret (from the Integration Record)
    - Token ID + Token Secret (from the Access Token)
 
    The requests-oauthlib library handles the signature generation automatically.
    """
 
    def __init__(self, config: NetSuiteConfig):
        self.config = config
 
    def get_auth(self):
        """Return an OAuth1 auth object for use with the requests library.
 
        Usage:
            auth = ns_auth.get_auth()
            response = requests.get(url, auth=auth)
        """
        # --- Uncomment for real usage ---
        # return OAuth1(
        #     client_key=self.config.consumer_key,
        #     client_secret=self.config.consumer_secret,
        #     resource_owner_key=self.config.token_id,
        #     resource_owner_secret=self.config.token_secret,
        #     realm=self.config.account_id,
        #     signature_method="HMAC-SHA256",
        # )
        logger.info(f"[DRY RUN] Would create OAuth1 auth for NetSuite account: {self.config.account_id}")
        return None
 
    def get_headers(self) -> dict:
        """Standard headers for NetSuite REST API calls."""
        return {
            "Content-Type": "application/json",
            "Prefer": "respond-async",  # For large operations
        }
 
 
# =============================================================================
# 3. NETSUITE METADATA DISCOVERY SERVICE
# =============================================================================
 
class NetSuiteDiscoveryService:
    """Discovers metadata from NetSuite using the SuiteTalk REST Metadata Catalog.
 
    Key NetSuite REST API endpoints:
    - GET /services/rest/record/v1/metadata-catalog/
      Returns a list of all available record types with their metadata.
    - GET /services/rest/record/v1/metadata-catalog/{recordType}
      Returns the full JSON Schema for a record type, including all fields,
      data types, required/optional status, sublists, and relationships.
    - POST /services/rest/query/v1/suiteql
      Runs SuiteQL queries for record counts and data discovery.
 
    Unlike Workday (which requires fetching sample records to discover fields),
    NetSuite provides a dedicated Metadata Catalog endpoint based on OpenAPI 3.0
    that describes every field, sublist, and relationship for each record type.
    """
 
    def __init__(self, ns_config: NetSuiteConfig, ns_auth: NetSuiteAuthService):
        self.config = ns_config
        self.auth = ns_auth
 
    def discover_record_fields(self, record_type: str) -> list:
        """Discover the field structure of a NetSuite record type via the Metadata Catalog.
 
        Args:
            record_type: The NetSuite record type (e.g., "customer", "salesOrder").
 
        Returns:
            List of field metadata dicts: [{name, type, label, required, ...}, ...]
        """
        # --- Uncomment for real usage ---
        # url = f"{self.config.record_api_url}/metadata-catalog/{record_type}"
        # response = requests.get(url, auth=self.auth.get_auth(), headers=self.auth.get_headers())
        # response.raise_for_status()
        # schema = response.json()
        #
        # # Parse the JSON Schema to extract field definitions
        # properties = schema.get("properties", {})
        # required_fields = schema.get("required", [])
        # fields = []
        # for field_name, field_def in properties.items():
        #     fields.append({
        #         "name": field_name,
        #         "type": field_def.get("type", "string"),
        #         "title": field_def.get("title", field_name),
        #         "required": field_name in required_fields,
        #         "readOnly": field_def.get("readOnly", False),
        #         "enum": field_def.get("enum", None),
        #     })
        # return fields
 
        # Validate record_type against the allow-list before it enters a URL.
        _validate_identifier(record_type, list(SUITEQL_TABLE_MAP.keys()))
        url = f"{self.config.record_api_url}/metadata-catalog/{record_type}"
        response = _request_with_retry(
            "GET", url, auth=self.auth.get_auth(), headers=self.auth.get_headers(),
            dry_run_payload=lambda: {"fields": self._get_simulated_fields(record_type)},
        )
        return response.json()["fields"]
 
    def get_record_count(self, record_type: str) -> int:
        """Get the record count via SuiteQL query.
 
        Uses: POST /services/rest/query/v1/suiteql
        Body: {"q": "SELECT COUNT(*) AS cnt FROM {table}"}
        """
        # --- Uncomment for real usage ---
        # table_map = {
        #     "customer": "customer", "vendor": "vendor", "employee": "employee",
        #     "salesOrder": "transaction WHERE type = 'SalesOrd'",
        #     "invoice": "transaction WHERE type = 'CustInvc'",
        #     "purchaseOrder": "transaction WHERE type = 'PurchOrd'",
        #     "vendorBill": "transaction WHERE type = 'VendBill'",
        #     "inventoryItem": "item WHERE itemType = 'InvtPart'",
        #     "journalEntry": "transaction WHERE type = 'Journal'",
        #     "account": "account",
        # }
        # table = table_map.get(record_type, record_type)
        # url = self.config.suiteql_url
        # payload = {"q": f"SELECT COUNT(*) AS cnt FROM {table}"}
        # response = requests.post(url, json=payload, auth=self.auth.get_auth(),
        #                          headers={**self.auth.get_headers(), "Prefer": "transient"})
        # response.raise_for_status()
        # items = response.json().get("items", [])
        # return items[0].get("cnt", 0) if items else 0
 
        # SuiteQL injection guard (active in dry-run and live): never interpolate
        # record_type into a query — only the pre-approved table expression from
        # SUITEQL_TABLE_MAP is used.
        if record_type not in SUITEQL_TABLE_MAP:
            raise ValueError(
                f"Record type '{record_type}' is not in the SuiteQL allow-list. "
                f"Add it to SUITEQL_TABLE_MAP before use."
            )
        table = SUITEQL_TABLE_MAP[record_type]
        counts = {
            "customer": 4200, "vendor": 850, "employee": 320,
            "salesOrder": 18500, "invoice": 22300, "purchaseOrder": 6100,
            "vendorBill": 9400, "inventoryItem": 3750,
            "journalEntry": 15200, "account": 285,
        }
        url = self.config.suiteql_url
        payload = {"q": f"SELECT COUNT(*) AS cnt FROM {table}"}
        response = _request_with_retry(
            "POST", url, json=payload, auth=self.auth.get_auth(),
            headers={**self.auth.get_headers(), "Prefer": "transient"},
            dry_run_payload=lambda: {"items": [{"cnt": counts.get(record_type, 0)}]},
        )
        items = response.json().get("items", [])
        return items[0].get("cnt", 0) if items else 0
 
    def _get_simulated_fields(self, record_type: str) -> list:
        """Return simulated field metadata for dry-run mode."""
        common_fields = [
            {"name": "id", "type": "integer", "title": "Internal ID", "required": True, "readOnly": True},
            {"name": "externalId", "type": "string", "title": "External ID", "required": False, "readOnly": False},
            {"name": "dateCreated", "type": "string", "title": "Date Created", "required": False, "readOnly": True},
            {"name": "lastModifiedDate", "type": "string", "title": "Last Modified", "required": False, "readOnly": True},
        ]
 
        record_fields = {
            "customer": [
                {"name": "companyName", "type": "string", "title": "Company Name", "required": False, "readOnly": False},
                {"name": "email", "type": "string", "title": "Email", "required": False, "readOnly": False},
                {"name": "phone", "type": "string", "title": "Phone", "required": False, "readOnly": False},
                {"name": "category", "type": "object", "title": "Category", "required": False, "readOnly": False},
                {"name": "subsidiary", "type": "object", "title": "Subsidiary", "required": True, "readOnly": False},
                {"name": "terms", "type": "object", "title": "Payment Terms", "required": False, "readOnly": False},
                {"name": "creditLimit", "type": "number", "title": "Credit Limit", "required": False, "readOnly": False},
                {"name": "balance", "type": "number", "title": "Balance", "required": False, "readOnly": True},
                {"name": "currency", "type": "object", "title": "Currency", "required": False, "readOnly": False},
            ],
            "vendor": [
                {"name": "companyName", "type": "string", "title": "Company Name", "required": False, "readOnly": False},
                {"name": "email", "type": "string", "title": "Email", "required": False, "readOnly": False},
                {"name": "subsidiary", "type": "object", "title": "Subsidiary", "required": True, "readOnly": False},
                {"name": "terms", "type": "object", "title": "Payment Terms", "required": False, "readOnly": False},
                {"name": "balance", "type": "number", "title": "Balance", "required": False, "readOnly": True},
                {"name": "taxIdNum", "type": "string", "title": "Tax ID Number", "required": False, "readOnly": False},
            ],
            "salesOrder": [
                {"name": "tranId", "type": "string", "title": "Transaction Number", "required": False, "readOnly": True},
                {"name": "entity", "type": "object", "title": "Customer", "required": True, "readOnly": False},
                {"name": "tranDate", "type": "string", "title": "Transaction Date", "required": True, "readOnly": False},
                {"name": "status", "type": "object", "title": "Status", "required": False, "readOnly": True},
                {"name": "subsidiary", "type": "object", "title": "Subsidiary", "required": True, "readOnly": False},
                {"name": "total", "type": "number", "title": "Total", "required": False, "readOnly": True},
                {"name": "currency", "type": "object", "title": "Currency", "required": False, "readOnly": False},
            ],
            "invoice": [
                {"name": "tranId", "type": "string", "title": "Invoice Number", "required": False, "readOnly": True},
                {"name": "entity", "type": "object", "title": "Customer", "required": True, "readOnly": False},
                {"name": "tranDate", "type": "string", "title": "Invoice Date", "required": True, "readOnly": False},
                {"name": "dueDate", "type": "string", "title": "Due Date", "required": False, "readOnly": False},
                {"name": "status", "type": "object", "title": "Status", "required": False, "readOnly": True},
                {"name": "total", "type": "number", "title": "Total", "required": False, "readOnly": True},
                {"name": "amountRemaining", "type": "number", "title": "Amount Remaining", "required": False, "readOnly": True},
            ],
            "inventoryItem": [
                {"name": "itemId", "type": "string", "title": "Item Name/Number", "required": True, "readOnly": False},
                {"name": "displayName", "type": "string", "title": "Display Name", "required": False, "readOnly": False},
                {"name": "cost", "type": "number", "title": "Cost", "required": False, "readOnly": False},
                {"name": "basePrice", "type": "number", "title": "Base Price", "required": False, "readOnly": False},
                {"name": "quantityOnHand", "type": "number", "title": "Quantity on Hand", "required": False, "readOnly": True},
                {"name": "quantityAvailable", "type": "number", "title": "Quantity Available", "required": False, "readOnly": True},
                {"name": "itemType", "type": "string", "title": "Item Type", "required": False, "readOnly": True},
            ],
            "account": [
                {"name": "acctNumber", "type": "string", "title": "Account Number", "required": False, "readOnly": False},
                {"name": "acctName", "type": "string", "title": "Account Name", "required": True, "readOnly": False},
                {"name": "acctType", "type": "object", "title": "Account Type", "required": True, "readOnly": False},
                {"name": "balance", "type": "number", "title": "Balance", "required": False, "readOnly": True},
                {"name": "currency", "type": "object", "title": "Currency", "required": False, "readOnly": False},
                {"name": "subsidiary", "type": "object", "title": "Subsidiary", "required": False, "readOnly": False},
            ],
        }
 
        extra = record_fields.get(record_type, [
            {"name": "entity", "type": "object", "title": "Entity", "required": False, "readOnly": False},
            {"name": "tranDate", "type": "string", "title": "Date", "required": False, "readOnly": False},
            {"name": "total", "type": "number", "title": "Total", "required": False, "readOnly": True},
        ])
        return common_fields + extra
 
 
# =============================================================================
# 4. PURVIEW TYPE DEFINITION SERVICE
# =============================================================================
 
class TypeDefService:
    """Manages custom type definitions in Purview for NetSuite assets."""
 
    NETSUITE_TYPES = {
        "entityDefs": [
            {
                "category": "ENTITY", "name": "custom_netsuite_account",
                "description": "An Oracle NetSuite account (instance)",
                "superTypes": ["Server"], "typeVersion": "1.0",
                "attributeDefs": [
                    {"name": "accountId", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
                    {"name": "accountUrl", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
                    {"name": "environment", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": True,
                     "description": "production, sandbox"},
                ],
            },
            {
                "category": "ENTITY", "name": "custom_netsuite_record_type",
                "description": "A NetSuite record type (customer, salesOrder, etc.)",
                "superTypes": ["DataSet"], "typeVersion": "1.0",
                "attributeDefs": [
                    {"name": "recordTypeName", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
                    {"name": "recordCategory", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": True,
                     "description": "Lists, Transactions, Items, etc."},
                    {"name": "recordCount", "typeName": "long", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
                ],
            },
            {
                "category": "ENTITY", "name": "custom_netsuite_field",
                "description": "A field on a NetSuite record type",
                "superTypes": ["DataSet"], "typeVersion": "1.0",
                "attributeDefs": [
                    {"name": "fieldType", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
                    {"name": "isRequired", "typeName": "boolean", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": False},
                    {"name": "isReadOnly", "typeName": "boolean", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": False},
                    {"name": "isReference", "typeName": "boolean", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": False},
                ],
            },
            {
                "category": "ENTITY", "name": "custom_netsuite_process",
                "description": "A data movement process from NetSuite to another system",
                "superTypes": ["Process"], "typeVersion": "1.0",
                "attributeDefs": [
                    {"name": "processType", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
                    {"name": "schedule", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "isUnique": False, "isIndexable": False},
                ],
            },
        ]
    }
 
    # Rollback support: stamp every custom type with a searchable
    # scanRunId attribute so entities from a specific run can be found
    # and deleted by purview_rollback_scan_run.py.
    _SCAN_RUN_ATTR = {
        "name": "scanRunId", "typeName": "string", "isOptional": True,
        "cardinality": "SINGLE", "isUnique": False, "isIndexable": True,
        "description": "ID of the scan run that last wrote this entity",
    }
    # Provenance marker (Security Review 8.2): which connector wrote this
    # entity, so downstream consumers can treat machine-synced text as such.
    _SOURCE_OF_TRUTH_ATTR = {
        "name": "sourceOfTruth", "typeName": "string", "isOptional": True,
        "cardinality": "SINGLE", "isUnique": False, "isIndexable": True,
        "description": "Provenance marker: which connector wrote this entity",
    }
    for _t in NETSUITE_TYPES["entityDefs"]:
        _t["attributeDefs"].append(dict(_SCAN_RUN_ATTR))
        _t["attributeDefs"].append(dict(_SOURCE_OF_TRUTH_ATTR))
    del _t
 
    @staticmethod
    def register_types(purview_endpoint: str, bearer_token: str) -> dict:
        url = f"{purview_endpoint}/api/atlas/v2/types/typedefs"
        type_names = [t["name"] for t in TypeDefService.NETSUITE_TYPES["entityDefs"]]
        headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/json"}
        _request_with_retry("POST", url, headers=headers, json=TypeDefService.NETSUITE_TYPES,
                            dry_run_payload=lambda: TypeDefService.NETSUITE_TYPES)
        logger.info(f"Registered {len(type_names)} types: {type_names}")
        return TypeDefService.NETSUITE_TYPES
 
 
# =============================================================================
# 5-7. ENTITY, LINEAGE, AND METADATA SERVICES
# =============================================================================
 
class EntityService:
    BATCH_SIZE = 50
 
    @staticmethod
    def build_entity(type_name, qualified_name, name, description="", attributes=None, classifications=None):
        entity = {"typeName": type_name, "attributes": {
            "qualifiedName": qualified_name,
            # M1: names/descriptions are untrusted source metadata
            "name": _sanitize_text(name, MAX_NAME_COMPONENT_LENGTH),
            "description": _sanitize_text(description), "scanRunId": SCAN_RUN_ID,  # rollback support
            "sourceOfTruth": SOURCE_OF_TRUTH,  # provenance (Security Review 8.2)
            **(attributes or {}),
        }, "status": "ACTIVE"}
        # Classifications derive from the shared ClassificationEngine
        if classifications:
            entity["classifications"] = [{"typeName": c} for c in classifications]
        return entity
 
    @staticmethod
    def create_entities_bulk(purview_endpoint, bearer_token, entities):
        url = f"{purview_endpoint}/api/atlas/v2/entity/bulk"
        for i in range(0, len(entities), EntityService.BATCH_SIZE):
            batch = entities[i : i + EntityService.BATCH_SIZE]
            headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/json"}
            _request_with_retry("POST", url, headers=headers, json={"entities": batch},
                                dry_run_payload=lambda b=batch: {"count": len(b)})
            logger.info(f"POSTed batch of {len(batch)} entities to {url}")
            for e in batch:
                logger.info(f"  → {e['attributes']['qualifiedName']}")
        return {"batches_sent": (len(entities) + EntityService.BATCH_SIZE - 1) // EntityService.BATCH_SIZE}
 
 
class LineageService:
    @staticmethod
    def build_process_entity(qualified_name, name, process_type, input_qns, input_types,
                             output_qns, output_types, description="", schedule=""):
        inputs = [{"typeName": t, "uniqueAttributes": {"qualifiedName": qn}}
                  for qn, t in zip(input_qns, input_types)]
        outputs = [{"typeName": t, "uniqueAttributes": {"qualifiedName": qn}}
                   for qn, t in zip(output_qns, output_types)]
        return {"typeName": "custom_netsuite_process", "attributes": {
            "qualifiedName": qualified_name, "name": name, "description": description,
            "processType": process_type, "schedule": schedule,
            "inputs": inputs, "outputs": outputs,
        }, "status": "ACTIVE"}
 
 
class MetadataService:
    @staticmethod
    def apply_business_metadata(purview_endpoint, bearer_token, entity_guid, metadata):
        headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/json"}
        url = f"{purview_endpoint}/api/atlas/v2/entity/guid/{entity_guid}/businessmetadata?isOverwrite=true"
        _request_with_retry("POST", url, headers=headers, json=metadata,
                            dry_run_payload=lambda: {"guid": entity_guid})
        logger.info(f"Applied business metadata to entity {entity_guid}: {metadata}")
 
    @staticmethod
    def apply_classification(purview_endpoint, bearer_token, entity_guid, classification_name):
        headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/json"}
        url = f"{purview_endpoint}/api/atlas/v2/entity/guid/{entity_guid}/classifications"
        _request_with_retry("POST", url, headers=headers, json=[{"typeName": classification_name}],
                            dry_run_payload=lambda: {"guid": entity_guid})
        logger.info(f"Applied classification '{classification_name}' to entity {entity_guid}")
 
 
# =============================================================================
# 8. NETSUITE CONNECTOR (ORCHESTRATOR)
# =============================================================================
 
class NetSuiteConnector:
    """Main connector that orchestrates the full NetSuite → Purview flow."""
 
    def __init__(self, purview_config, netsuite_config, account_name="mycompany-netsuite"):
        self.purview_config = purview_config
        self.ns_config = netsuite_config
        self.account_name = account_name
        self.purview_auth = PurviewAuthService(purview_config)
        self.ns_auth = NetSuiteAuthService(netsuite_config)
        self.discovery = NetSuiteDiscoveryService(netsuite_config, self.ns_auth)
 
    def run(self, record_types=None, lineage_mappings=None):
        record_types = record_types or RECORD_TYPES_TO_SCAN
        lineage_mappings = lineage_mappings or LINEAGE_MAPPINGS
 
        logger.info("=" * 70)
        logger.info("ORACLE NETSUITE → PURVIEW CUSTOM CONNECTOR")
        logger.info("=" * 70)
 
        # Step 1: Authenticate
        logger.info("\n--- Step 1: Authentication ---")
        purview_token = self.purview_auth.get_bearer_token()
        self.ns_auth.get_auth()  # Validate OAuth credentials
        purview_endpoint = self.purview_config.endpoint
 
        # Step 2: Register types
        logger.info("\n--- Step 2: Register NetSuite Custom Types in Purview ---")
        TypeDefService.register_types(purview_endpoint, purview_token)
 
        # Step 3: Discover metadata
        logger.info("\n--- Step 3: Discover NetSuite Metadata ---")
        # LLM10: cap discovery breadth (env-configurable; skips are logged)
        record_types = _apply_cap(record_types, MAX_OBJECTS_PER_RUN, "NetSuite record types")
 
        # Classifications are driven by classification_rules.json via the
        # shared ClassificationEngine — no hardcoded classification logic.
        classification_engine = ClassificationEngine()
        logger.info(f"Classification engine loaded: {classification_engine.get_stats()['total']} rules")
        all_entities = []
        record_details = {}
 
        # Account-level entity
        acct_qn = f"netsuite://{self.account_name}"
        all_entities.append(EntityService.build_entity(
            "custom_netsuite_account", acct_qn,
            f"NetSuite - {self.account_name}",
            f"Oracle NetSuite account: {self.ns_config.account_id}",
            {"accountId": self.ns_config.account_id,
             "accountUrl": self.ns_config.base_url, "environment": "production"},
        ))
 
        for rec in record_types:
            rt = _safe_name_component(rec["record_type"])  # M1: untrusted source metadata
            fields = _apply_cap(self.discovery.discover_record_fields(rt),
                                MAX_FIELDS_PER_OBJECT, f"fields on {rt}")  # LLM10
            record_details[rt] = fields
            count = self.discovery.get_record_count(rt)
 
            rec_qn = f"netsuite://{self.account_name}/{rt}"
            all_entities.append(EntityService.build_entity(
                "custom_netsuite_record_type", rec_qn, rec["display_name"],
                rec["description"],
                {"recordTypeName": rt, "recordCategory": rec["category"], "recordCount": count},
            ))
 
            for fld in fields:
                fname = _safe_name_component(fld["name"])  # M1
                fld_qn = f"netsuite://{self.account_name}/{rt}/{fname}"
                fclass = classification_engine.classify_field(
                    source="netsuite", object_name=rt,
                    field_name=fname, field_type=fld.get("type"))
                all_entities.append(EntityService.build_entity(
                    "custom_netsuite_field", fld_qn,
                    fld.get("title", fname),
                    f"Field {fname} on {rt} (type: {fld['type']})",
                    {"fieldType": fld["type"], "isRequired": fld.get("required", False),
                     "isReadOnly": fld.get("readOnly", False),
                     "isReference": fld["type"] == "object"},
                    classifications=[fclass] if fclass else None,
                ))
 
        # LLM04: warn if asset descriptions drifted since the last run
        _check_metadata_drift(
            (e["attributes"]["qualifiedName"], e["attributes"].get("description", ""))
            for e in all_entities
        )
 
        # Step 4: Push entities
        logger.info(f"\n--- Step 4: Create {len(all_entities)} Entities in Purview ---")
        EntityService.create_entities_bulk(purview_endpoint, purview_token, all_entities)
 
        # Step 5: Build lineage
        logger.info("\n--- Step 5: Build Cross-System Lineage ---")
        process_entities = []
        for mapping in lineage_mappings:
            source_qns = [f"netsuite://{self.account_name}/{r}" for r in mapping["source_records"]]
            source_types = ["custom_netsuite_record_type"] * len(mapping["source_records"])
            process_qn = f"netsuite://{self.account_name}/process/{_safe_name_component(mapping['process_name'].replace(' ', '_').lower())}"  # M1
 
            process_entities.append(LineageService.build_process_entity(
                process_qn, mapping["process_name"], mapping["process_type"],
                source_qns, source_types, [mapping["destination_table"]],
                [mapping["destination_type"]], mapping.get("description", ""),
                "Daily 3:00 AM UTC",
            ))
            logger.info(f"  Lineage: {mapping['source_records']} → {mapping['process_name']} → {mapping['destination_table']}")
 
        EntityService.create_entities_bulk(purview_endpoint, purview_token, process_entities)
 
        # Step 6: Apply metadata and classifications
        logger.info("\n--- Step 6: Apply Business Metadata and Classifications ---")
        if APPLY_BUSINESS_METADATA:
            # ⚠️ M3 PLACEHOLDER VALUES: replace with values computed from real
            # sources before enabling the flag (see Security Review).
            MetadataService.apply_business_metadata(
                purview_endpoint, purview_token, "dry-run-guid-customer-001",
                {"DataQuality": {"lastValidated": "2026-02-20T03:00:00Z", "qualityScore": 91.8,
                                 "dataOwner": "Finance Operations", "dataSteward": "ERP Admin"}},
            )
        else:
            logger.info(
                "Skipping business metadata (APPLY_BUSINESS_METADATA is not 'true'). "
                "Replace the placeholder values with values computed from real "
                "sources (data-quality jobs, ownership registries) before enabling."
            )
 
        # Classifications were attached at entity-build time by the shared
        # ClassificationEngine (classification_rules.json) — no hardcoded
        # sensitive-field lists. Log what the engine classified.
        classified = [e for e in all_entities if e.get("classifications")]
        for e in classified:
            logger.info(f"  Classified {e['attributes']['qualifiedName']} -> "
                        f"{[c['typeName'] for c in e['classifications']]}")
 
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("CONNECTOR RUN COMPLETE")
        logger.info("=" * 70)
        logger.info(f"  Account entity:        1")
        logger.info(f"  Record type entities:  {len(record_types)}")
        field_count = sum(len(record_details[r["record_type"]]) for r in record_types)
        logger.info(f"  Field entities:        {field_count}")
        logger.info(f"  Process entities:      {len(process_entities)}")
        logger.info(f"  Total entities:        {len(all_entities) + len(process_entities)}")
        logger.info(f"  Fields classified (engine): {len(classified)}")
 
 
# =============================================================================
# 9. MAIN ENTRY POINT
# =============================================================================
 
def main():
    logger.info("Initializing Oracle NetSuite → Purview custom connector...")
    purview_config = PurviewConfig.from_environment()
    kv_url = os.environ.get("KEY_VAULT_URL", "https://my-keyvault.vault.azure.net/")
    ns_config = NetSuiteConfig.from_key_vault(kv_url)
 
    connector = NetSuiteConnector(
        purview_config=purview_config,
        netsuite_config=ns_config,
        account_name="mycompany-netsuite",
    )
    connector.run(record_types=RECORD_TYPES_TO_SCAN, lineage_mappings=LINEAGE_MAPPINGS)
 
 
if __name__ == "__main__":
    main()
 
