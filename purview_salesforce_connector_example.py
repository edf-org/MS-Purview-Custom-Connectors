"""
Microsoft Purview Custom Connector for Salesforce - Example Implementation
============================================================================
 
This example demonstrates a complete custom connector that:
1. Authenticates to Salesforce via OAuth 2.0 Client Credentials Flow
2. Authenticates to Purview using Managed Identity / Service Principal
3. Discovers Salesforce objects and fields via the REST API (Describe Global + sObject Describe)
4. Registers custom type definitions in Purview for Salesforce assets
5. Creates entities (org → objects → fields hierarchy) in Purview via Atlas v2
6. Builds cross-system lineage (Salesforce → ETL → Data Warehouse)
7. Applies business metadata and classifications
 
This is the SAME approach used for Workday and SQL Server — the only difference
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
 
       pip install pyapacheatlas azure-identity azure-keyvault-secrets requests python-dotenv
 
   If deploying to Azure Functions, add these to your requirements.txt file:
 
       azure-functions>=1.17.0
       pyapacheatlas>=0.14.0
       azure-identity>=1.15.0
       azure-keyvault-secrets>=4.8.0
       requests>=2.31.0
       python-dotenv>=1.0.0
 
3. A Microsoft Purview account (Data Map enabled) in your Azure subscription.
   The connector pushes metadata into Purview, so Purview must be provisioned first.
   https://learn.microsoft.com/en-us/purview/create-microsoft-purview-portal
 
4. An Azure Key Vault to securely store Salesforce credentials.
   If you do not have one, create it in the Azure portal:
   Azure portal > Create a resource > Key Vault > Create.
   https://learn.microsoft.com/en-us/azure/key-vault/general/quick-create-portal
 
5. A Salesforce org (Production, Sandbox, or Developer Edition) with API access enabled.
   The connector reads metadata via the Salesforce REST API; it does not modify any
   Salesforce data.
 
6. A Salesforce Connected App configured for the OAuth 2.0 Client Credentials Flow.
   This is how the connector authenticates to Salesforce without a username/password.
   Setup steps (see Architecture Document Section 3.3 for full detail):
     a. In Salesforce Setup, go to App Manager > New Connected App.
     b. Enable OAuth Settings with the "Manage user data via APIs (api)" scope.
     c. Check "Enable Client Credentials Flow".
     d. After saving, go to Manage > Edit Policies > Client Credentials Flow section
        and assign a Run As user (a dedicated Integration User with API-only access).
     e. Under Manage Consumer Details, copy the Consumer Key and Consumer Secret.
 
7. Authentication to Purview — choose one of:
 
   a. Managed Identity (recommended for Azure-hosted deployments):
      - Enable System-Assigned Managed Identity on your Azure Function App.
      - Assign the identity "Data Curator" and "Data Source Administrator" roles
        in the Purview governance portal (Data Map > Collections > Role assignments).
      - Grant the identity "Key Vault Secrets User" role on your Azure Key Vault.
      - No environment variables needed for AZURE_TENANT_ID/CLIENT_ID/CLIENT_SECRET.
 
   b. Service Principal (required for local development or non-Azure environments):
      - Register an App in Microsoft Entra ID (Azure portal > App registrations).
      - Create a client secret.
      - Assign "Data Curator" and "Data Source Administrator" roles in the Purview
        governance portal.
      - Set the AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET
        environment variables (see below).
 
8. Where to run this connector:
 
   LOCAL DEVELOPMENT:
     - Run directly on your laptop/workstation with Python installed.
     - Use a .env file for environment variables (see below).
     - Uses Service Principal authentication (Managed Identity is not available locally).
     - Great for testing and validating the connector before deploying to Azure.
 
   AZURE FUNCTIONS (recommended for production):
     - Deploy as a Timer Trigger (scheduled) or HTTP Trigger (on-demand).
     - Uses Managed Identity for both Purview and Key Vault access — no secrets in code.
     - See Architecture Document Section 6 for Azure Functions deployment steps.
     - Project structure for Azure Functions:
         purview-salesforce-connector/
         ├── function_app.py                        # Timer + HTTP trigger definitions
         ├── host.json                              # Runtime config
         ├── local.settings.json                    # Local dev settings (DO NOT commit)
         ├── requirements.txt                       # Python dependencies
         └── purview_salesforce_connector_example.py # This file (imported by function_app.py)
 
   AZURE CONTAINER APPS:
     - Alternative if connector jobs exceed Azure Functions timeout limits.
     - Containerize with Docker; deploy to Azure Container Apps with Managed Identity.
 
   ON-PREMISES SERVER:
     - Run as a scheduled task (cron on Linux, Task Scheduler on Windows).
     - Must use Service Principal authentication.
     - Must have network access to both Salesforce (internet) and Purview (internet or
       private endpoint).
 
 
ENVIRONMENT VARIABLES
---------------------
Set these as environment variables before running the connector. In local development,
create a .env file in the same directory as this script. In Azure Functions, set them
as Application Settings (Function App > Configuration > Application settings).
 
Required for ALL environments:
    PURVIEW_ACCOUNT_NAME   Your Purview account name, WITHOUT the domain suffix.
                           Example: "my-purview-account"
                           (NOT "my-purview-account.purview.azure.com")
                           Where to find it: Azure portal > your Purview resource > Overview.
 
    KEY_VAULT_URL          The full URL of your Azure Key Vault.
                           Example: "https://my-keyvault.vault.azure.net/"
                           Where to find it: Azure portal > your Key Vault > Overview > Vault URI.
 
Required ONLY for Service Principal authentication (local dev / non-Azure):
    AZURE_TENANT_ID        Your Microsoft Entra ID (Azure AD) tenant ID.
                           Example: "12a345bc-67d1-ef89-abcd-efg12345abcde"
                           Where to find it: Azure portal > Microsoft Entra ID > Overview,
                           or App Registration > Overview > Directory (tenant) ID.
 
    AZURE_CLIENT_ID        The Application (client) ID of your registered app.
                           Example: "a1234bcd-5678-9012-abcd-abcd1234abcd"
                           Where to find it: Azure portal > App registrations >
                           your app > Overview > Application (client) ID.
 
    AZURE_CLIENT_SECRET    The client secret value for your registered app.
                           Example: "xYz...your-secret-value..."
                           Where to find it: App Registration > Certificates & secrets.
                           IMPORTANT: This value is only shown once at creation time.
                           If you lost it, create a new secret.
 
NOT required when using Managed Identity in Azure:
    When running in Azure Functions with Managed Identity enabled,
    DefaultAzureCredential automatically detects and uses the managed identity.
    You do NOT need to set AZURE_TENANT_ID, AZURE_CLIENT_ID, or AZURE_CLIENT_SECRET.
    You still need PURVIEW_ACCOUNT_NAME and KEY_VAULT_URL.
 
Example .env file for local development:
    # .env — DO NOT COMMIT TO SOURCE CONTROL (add to .gitignore)
    AZURE_TENANT_ID=12a345bc-67d1-ef89-abcd-efg12345abcde
    AZURE_CLIENT_ID=a1234bcd-5678-9012-abcd-abcd1234abcd
    AZURE_CLIENT_SECRET=xYz...your-secret-value...
    PURVIEW_ACCOUNT_NAME=my-purview-account
    KEY_VAULT_URL=https://my-keyvault.vault.azure.net/
 
 
AZURE KEY VAULT SECRETS REQUIRED
---------------------------------
The following secrets must be created in your Azure Key Vault BEFORE running
the connector. The connector retrieves these at runtime using Managed Identity
(in Azure) or Service Principal credentials (locally).
 
To create secrets in Azure Key Vault:
    Azure portal > your Key Vault > Objects > Secrets > + Generate/Import
    Set the Name and Value for each secret below, then click Create.
 
    Secret Name                     Description                                      Example Value
    ----------------------------    -----------------------------------------------  ---------------------------
    salesforce-consumer-key         The Consumer Key (Client ID) from your           3MVG9pRzvMkjMb6l...
                                    Salesforce Connected App. Found under
                                    App Manager > your app > Manage Consumer
                                    Details > Consumer Key.
 
    salesforce-consumer-secret      The Consumer Secret (Client Secret) from your    E8B1C9A2D4F6...
                                    Salesforce Connected App. Found under
                                    App Manager > your app > Manage Consumer
                                    Details > Consumer Secret.
 
    salesforce-domain-url           Your Salesforce My Domain URL. This is the       https://mycompany.my.salesforce.com
                                    base URL used for API calls and OAuth.
                                    Found under Setup > My Domain.
                                    For sandboxes, use the sandbox-specific URL:
                                    https://mycompany--sandbox1.sandbox.my.salesforce.com
 
Granting the connector access to read these secrets:
    - If using Managed Identity: In the Azure portal, go to your Key Vault >
      Access control (IAM) > Add role assignment > Role: "Key Vault Secrets User" >
      Members: select your Azure Function App name > Review + assign.
    - If using Service Principal: Same process, but select your App Registration
      name instead of the Function App name.
 
 
USAGE
-----
This connector runs in DRY-RUN MODE by default — it simulates all Salesforce API
calls and Purview API calls, logging what it WOULD do without making any actual
requests. This lets you validate the logic and entity structure before connecting
to real systems.
 
1. DRY-RUN MODE (default, no credentials needed):
   Simply run the script to see the simulated output:
 
       python purview_salesforce_connector_example.py
 
   Review the log output. You will see:
   - Which Salesforce objects and fields would be discovered
   - Which Purview entity types would be registered
   - Which entities (org, objects, fields) would be created and their qualifiedNames
   - Which lineage processes would be created and their input/output mappings
   - Which business metadata and classifications would be applied
 
2. LIVE MODE (requires real credentials and uncommenting code):
   To run against real Salesforce and Purview instances:
 
   a. Set up all prerequisites listed above (Purview, Key Vault, Salesforce Connected App).
   b. Create the Azure Key Vault secrets listed above.
   c. Set the environment variables listed above (via .env file or Azure Function App Settings).
   d. In this file, uncomment all lines marked with "--- Uncomment for real usage ---"
      and "--- Uncomment these imports ---" at the top of the file.
   e. Comment out or remove the "[DRY RUN]" simulation blocks.
   f. Run the script:
 
       python purview_salesforce_connector_example.py
 
3. AZURE FUNCTIONS DEPLOYMENT:
   To deploy as a scheduled Azure Function:
 
   a. Create an Azure Function App (Python 3.11+, same region as Purview).
   b. Enable System-Assigned Managed Identity on the Function App.
   c. Grant the Managed Identity "Data Curator" + "Data Source Administrator" roles
      in the Purview governance portal.
   d. Grant the Managed Identity "Key Vault Secrets User" role on your Key Vault.
   e. Set PURVIEW_ACCOUNT_NAME and KEY_VAULT_URL in Function App > Configuration.
   f. Create a function_app.py that imports and calls this connector:
 
       import azure.functions as func
       from purview_salesforce_connector_example import main as run_connector
 
       app = func.FunctionApp()
 
       @app.timer_trigger(schedule="0 0 2 * * *", arg_name="timer")
       def purview_salesforce_scan(timer: func.TimerRequest) -> None:
           run_connector()
 
   g. Deploy using: func azure functionapp publish <your-function-app-name>
   h. Monitor execution in Azure portal > Function App > Monitor > Logs.
 
4. CUSTOMIZATION:
   - To change which Salesforce objects are scanned, edit the OBJECTS_TO_SCAN list
     in the Configuration section below.
   - To change the cross-system lineage mappings, edit the LINEAGE_MAPPINGS list.
   - To add new business metadata or classifications, modify the
     SalesforceConnector.run() method in Step 6.
   - To scan ALL queryable objects (not just the predefined list), set
     OBJECTS_TO_SCAN = None and the connector will use Describe Global to discover
     every object in the org.
"""
 
import json
import logging
import os
import re
import sys
import urllib.parse
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
 
# Shared, configuration-driven classification engine (rules live in
# classification_rules.json — maintained by data stewards)
from classification_engine import ClassificationEngine
 
# --- Uncomment these imports when running against real Purview + Salesforce ---
# from pyapacheatlas.auth import ServicePrincipalAuthentication
# from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess
# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
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
# Tuple format: (connect_timeout, read_timeout). Prevents indefinite hangs
# if a remote service becomes unresponsive. Applied by _request_with_retry to
# every call — source-side (Salesforce) and Purview-side alike.
REQUEST_TIMEOUT = (10, 30)


# Dry-run transport toggle. When true (the default for these examples), HTTP
# calls are simulated by _DryRunResponse *through the same request path* used
# in live mode — so identifier/URL validation, the timeout, and the
# retry/backoff wrapper are all genuinely exercised without real credentials.
# Live mode: set CONNECTOR_DRY_RUN=false and uncomment `import requests` above.
DRY_RUN = os.environ.get("CONNECTOR_DRY_RUN", "true").strip().lower() != "false"


# --- Input validation: SOQL/path injection + SSRF guards (ported from the
# remote reference connector). These run on the live call path in front of the
# retry wrapper, so a malicious object name or tampered domain URL fails before
# any request is issued. ---

def _validate_identifier(value: str, allow_list: list = None) -> str:
    """Validate that a string is a safe API/SOQL identifier.

    Prevents injection by allowing only letters, digits, and underscores —
    rejecting semicolons, quotes, whitespace, and SOQL comment sequences.
    Optionally validates against an explicit allow-list (e.g. the objects
    actually requested for this scan). Raises ValueError if unsafe.
    """
    if allow_list and value not in allow_list:
        raise ValueError(
            f"Identifier '{value}' is not in the allow-list. "
            f"Allowed values: {allow_list}"
        )
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', value):
        raise ValueError(
            f"Identifier '{value}' contains invalid characters. "
            f"Only alphanumeric characters and underscores are allowed."
        )
    return value


def _validate_url_domain(url: str, expected_suffix: str) -> str:
    """Validate that a URL uses HTTPS and its host ends with expected_suffix.

    Prevents SSRF if a URL value retrieved from Key Vault has been tampered
    with or misconfigured to point to an attacker-controlled host. Raises
    ValueError if the URL fails validation.
    """
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"URL '{url}' must use HTTPS.")
    # Use hostname (not netloc) so userinfo/port components cannot confuse the check.
    host = (parsed.hostname or "").lower()
    if not host.endswith(expected_suffix):
        raise ValueError(
            f"URL '{url}' host does not end with expected domain '{expected_suffix}'. "
            f"Verify the value stored in Key Vault."
        )
    return url
 
 
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

    Lets the full request path — identifier/URL validation, the configured
    timeout, and the retry/backoff wrapper — execute unchanged without a live
    endpoint or the `requests` dependency. status_code is 200 so the wrapper's
    success path runs; json() returns the simulated payload.
    """

    def __init__(self, payload, url: str, method: str, timeout):
        self._payload = payload if payload is not None else {}
        self.url = url
        self.request_method = method
        self.request_timeout = timeout  # the REQUEST_TIMEOUT that was applied
        self.status_code = 200
        self.headers = {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


def _request_with_retry(method: str, url: str, dry_run_payload=None, **kwargs):
    """HTTP request with exponential backoff, used for BOTH live and dry-run.

    In live mode (DRY_RUN false) it calls requests.request; in dry-run it
    returns a _DryRunResponse built from `dry_run_payload` (a value or zero-arg
    callable), so the same validated, timed-out, retry-wrapped path is
    exercised without live credentials or the requests dependency.

    Retries on 429 (honoring Retry-After — important for shared source-API
    quotas like Salesforce daily limits), transient 5xx, and connection or
    timeout errors.

    Usage (replaces bare requests.get/post calls):
        response = _request_with_retry("GET", url, headers=headers,
                                       dry_run_payload=lambda: {...})
    """
    import time
    import random
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)
    for attempt in range(MAX_RETRIES + 1):
        try:
            if DRY_RUN:
                # Simulated transport: exercises identifier/URL validation, the
                # timeout, and this retry/backoff wrapper without a live
                # endpoint. The timeout that would be sent is logged and carried
                # on the response so it is demonstrably applied.
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
            # Check for transient network errors from requests library
            # Import check: requests may not be available in dry-run mode
            is_retryable = False
            if not DRY_RUN:
                import requests.exceptions
                is_retryable = isinstance(exc, (
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError,
                ))
            if is_retryable:
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
    read_succeeded = False
    if os.path.exists(state_path):
        try:
            with open(state_path) as fh:
                previous = _json.load(fh)
            read_succeeded = True
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
 
    # Only persist state in live mode and if the previous read succeeded
    if not DRY_RUN and (read_succeeded or not os.path.exists(state_path)):
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
SOURCE_OF_TRUTH = "connector:salesforce"
 
 
# =============================================================================
# 1. CONFIGURATION
# =============================================================================
 
SALESFORCE_API_VERSION = "v62.0"  # Spring '26 — update to match your org's version
 
# Which Salesforce objects to catalog. Set to None to discover all queryable objects.
# For a targeted scan, list specific object API names:
OBJECTS_TO_SCAN = [
    "Account",
    "Contact",
    "Opportunity",
    "Lead",
    "Case",
    "Campaign",
    "Order",
    "Product2",
    "Pricebook2",
    "Contract",
]
 
# Objects to include in cross-system lineage (Salesforce → Data Warehouse)
LINEAGE_MAPPINGS = [
    {
        "source_objects": ["Account", "Contact"],
        "process_name": "CRM Account Sync",
        "process_type": "ETL",
        "destination_table": "dwh://analytics-warehouse/crm/dim_customer",
        "destination_type": "custom_sql_table",
        "description": "Daily sync of Salesforce Account and Contact data to the customer dimension table",
    },
    {
        "source_objects": ["Opportunity"],
        "process_name": "Pipeline Revenue Sync",
        "process_type": "ETL",
        "destination_table": "dwh://analytics-warehouse/crm/fact_opportunity",
        "destination_type": "custom_sql_table",
        "description": "Daily sync of Salesforce Opportunity data to the opportunity fact table",
    },
    {
        "source_objects": ["Case"],
        "process_name": "Support Ticket Sync",
        "process_type": "ETL",
        "destination_table": "dwh://analytics-warehouse/support/fact_case",
        "destination_type": "custom_sql_table",
        "description": "Daily sync of Salesforce Case data to the case fact table",
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
        """Load configuration from environment variables."""
        # load_dotenv()  # Uncomment to load from .env file
        return cls(
            tenant_id=os.environ.get("AZURE_TENANT_ID", ""),
            client_id=os.environ.get("AZURE_CLIENT_ID", ""),
            client_secret=os.environ.get("AZURE_CLIENT_SECRET", ""),
            account_name=os.environ.get("PURVIEW_ACCOUNT_NAME", ""),
        )
 
    @property
    def endpoint(self) -> str:
        # L1: PURVIEW_ENDPOINT override supports the new unified portal, but
        # the host is allow-list validated so a tampered App Setting cannot
        # redirect the metadata stream to an untrusted destination.
        override = os.environ.get("PURVIEW_ENDPOINT", "").rstrip("/")
        if override:
            return _validate_purview_endpoint(override)  # L1: allow-list check
        return f"https://{self.account_name}.purview.azure.com/datamap"
 
 
@dataclass
class SalesforceConfig:
    """Configuration for connecting to Salesforce."""
    consumer_key: str = ""
    consumer_secret: str = ""
    domain_url: str = ""  # e.g., https://mycompany.my.salesforce.com
    instance_url: str = ""  # returned by OAuth token response
    access_token: str = ""  # returned by OAuth token response
    api_version: str = SALESFORCE_API_VERSION
 
    @classmethod
    def from_key_vault(cls, kv_url: str) -> "SalesforceConfig":
        """Load Salesforce credentials from Azure Key Vault using Managed Identity."""
        # --- Uncomment for real usage ---
        # credential = DefaultAzureCredential()
        # kv_client = SecretClient(vault_url=kv_url, credential=credential)
        # return cls(
        #     consumer_key=kv_client.get_secret("salesforce-consumer-key").value,
        #     consumer_secret=kv_client.get_secret("salesforce-consumer-secret").value,
        #     domain_url=kv_client.get_secret("salesforce-domain-url").value,
        # )
        logger.info(f"[DRY RUN] Would retrieve Salesforce credentials from Key Vault: {kv_url}")
        return cls(
            consumer_key="dry-run-consumer-key",
            consumer_secret="dry-run-consumer-secret",
            domain_url="https://mycompany.my.salesforce.com",
        )
 
    @property
    def base_api_url(self) -> str:
        """Base URL for Salesforce REST API calls."""
        url = self.instance_url or self.domain_url
        # SSRF guard: ensure the URL points to a Salesforce-owned domain. This
        # prevents a misconfigured or tampered Key Vault secret (domain-url)
        # from redirecting authenticated requests to an attacker-controlled
        # host. Runs on every call because base_api_url is recomputed per use.
        _validate_url_domain(url, ".salesforce.com")
        return f"{url}/services/data/{self.api_version}"
 
 
# =============================================================================
# 2. AUTHENTICATION SERVICES
# =============================================================================
 
class PurviewAuthService:
    """Handles authentication to Microsoft Purview.
 
    Supports two modes:
    - Service Principal (for local dev / non-Azure environments)
    - Managed Identity via DefaultAzureCredential (for Azure-hosted deployments)
    """
 
    def __init__(self, config: PurviewConfig):
        self.config = config
 
    def get_purview_client(self):  # -> PurviewClient
        """Create an authenticated PurviewClient using pyapacheatlas."""
        # --- Uncomment for real usage ---
        # auth = ServicePrincipalAuthentication(
        #     tenant_id=self.config.tenant_id,
        #     client_id=self.config.client_id,
        #     client_secret=self.config.client_secret,
        # )
        # return PurviewClient(
        #     account_name=self.config.account_name,
        #     authentication=auth,
        # )
        logger.info(f"[DRY RUN] Would authenticate to Purview account: {self.config.account_name}")
        return None
 
    def get_bearer_token(self) -> str:
        """Get a bearer token for direct REST API calls."""
        # --- Uncomment for real usage ---
        # credential = DefaultAzureCredential()
        # token = credential.get_token("https://purview.azure.net/.default")
        # return token.token
        logger.info("[DRY RUN] Would acquire Purview bearer token via DefaultAzureCredential")
        return "dry-run-purview-token"
 
 
class SalesforceAuthService:
    """Handles OAuth 2.0 Client Credentials authentication to Salesforce.
 
    The Client Credentials flow:
    1. Sends consumer key + consumer secret to Salesforce token endpoint
    2. Receives access_token + instance_url
    3. Uses access_token as Bearer token for all subsequent API calls
 
    No username, password, or security token required.
    """
 
    def __init__(self, config: SalesforceConfig):
        self.config = config
 
    def authenticate(self) -> SalesforceConfig:
        """Authenticate to Salesforce and return updated config with token."""
        # --- Uncomment for real usage ---
        # token_url = f"{self.config.domain_url}/services/oauth2/token"
        # response = requests.post(token_url, data={
        #     "grant_type": "client_credentials",
        #     "client_id": self.config.consumer_key,
        #     "client_secret": self.config.consumer_secret,
        # })
        # response.raise_for_status()
        # token_data = response.json()
        # self.config.access_token = token_data["access_token"]
        # self.config.instance_url = token_data["instance_url"]
        # logger.info(f"Authenticated to Salesforce instance: {self.config.instance_url}")
 
        logger.info(f"[DRY RUN] Would authenticate to Salesforce at: {self.config.domain_url}")
        self.config.access_token = "dry-run-sf-token"
        self.config.instance_url = self.config.domain_url
        return self.config
 
    def get_headers(self) -> dict:
        """Get HTTP headers for Salesforce API calls."""
        return {
            "Authorization": f"Bearer {self.config.access_token}",
            "Content-Type": "application/json",
        }
 
 
# =============================================================================
# 3. SALESFORCE METADATA DISCOVERY SERVICE
# =============================================================================
 
class SalesforceDiscoveryService:
    """Discovers metadata from Salesforce using the REST API.
 
    Uses two key Salesforce endpoints:
    - Describe Global (/services/data/vXX.0/sobjects/)
      Returns a list of all objects in the org with basic metadata.
    - sObject Describe (/services/data/vXX.0/sobjects/{ObjectName}/describe/)
      Returns full metadata for a single object, including all fields,
      child relationships, record type info, and URLs.
    """
 
    def __init__(self, sf_config: SalesforceConfig, sf_auth: SalesforceAuthService):
        self.config = sf_config
        self.auth = sf_auth
 
    def discover_objects(self, object_filter: list = None) -> list:
        """Discover Salesforce objects via Describe Global.
 
        Args:
            object_filter: Optional list of object API names to include.
                           If None, returns all queryable, non-deprecated objects.
 
        Returns:
            List of object metadata dicts with keys: name, label, custom, queryable, etc.
        """
        # --- Uncomment for real usage ---
        # url = f"{self.config.base_api_url}/sobjects/"
        # response = requests.get(url, headers=self.auth.get_headers())
        # response.raise_for_status()
        # all_objects = response.json()["sobjects"]
        #
        # # Filter to queryable, non-deprecated objects
        # objects = [
        #     obj for obj in all_objects
        #     if obj.get("queryable", False)
        #     and not obj.get("deprecatedAndHidden", False)
        # ]
        #
        # # Apply name filter if provided
        # if object_filter:
        #     objects = [obj for obj in objects if obj["name"] in object_filter]
        #
        # logger.info(f"Discovered {len(objects)} Salesforce objects")
        # return objects
 
        # Build the URL (base_api_url runs the _validate_url_domain SSRF guard)
        # and issue it through the retry/timeout wrapper. In dry-run the wrapper
        # returns the simulated Describe Global payload produced below; in live
        # mode the same call hits Salesforce.
        url = f"{self.config.base_api_url}/sobjects/"

        def _simulate():
            objects = []
            for name in (object_filter or OBJECTS_TO_SCAN):
                objects.append({
                    "name": name,
                    "label": name.replace("2", "").replace("_", " "),
                    "labelPlural": f"{name}s",
                    "custom": name.endswith("__c"),
                    "queryable": True,
                    "createable": True,
                    "updateable": True,
                    "deletable": True,
                    "keyPrefix": f"{name[:3].upper()}",
                })
            return {"sobjects": objects}

        response = _request_with_retry(
            "GET", url, headers=self.auth.get_headers(), dry_run_payload=_simulate
        )
        all_objects = response.json()["sobjects"]

        # Filter to queryable, non-deprecated objects (identical to live mode)
        objects = [
            obj for obj in all_objects
            if obj.get("queryable", False) and not obj.get("deprecatedAndHidden", False)
        ]
        if object_filter:
            objects = [obj for obj in objects if obj["name"] in object_filter]
        logger.info(f"Discovered {len(objects)} Salesforce objects")
        return objects
 
    def describe_object(self, object_name: str, allow_list: list = None) -> dict:
        """Get full metadata for a single Salesforce object via sObject Describe.
 
        Returns field-level detail including: name, label, type, length,
        precision, scale, nillable, unique, externalId, referenceTo, etc.
 
        Args:
            object_name: The API name of the Salesforce object (e.g., "Account").
 
        Returns:
            Full sObject Describe response dict.
        """
        # Validate the object name before it goes into a URL/SOQL — prevents
        # path/SOQL injection if object names are ever sourced from external
        # input. Runs in front of the retry wrapper so a bad name never issues
        # a request. allow_list, when supplied, restricts to this scan's objects.
        _validate_identifier(object_name, allow_list)

        # Build the URL (base_api_url runs the _validate_url_domain SSRF guard)
        # and issue it through the retry/timeout wrapper.
        url = f"{self.config.base_api_url}/sobjects/{object_name}/describe/"

        def _simulate():
            return {
                "name": object_name,
                "label": object_name.replace("2", "").replace("_", " "),
                "fields": self._get_simulated_fields(object_name),
                "childRelationships": [],
                "recordTypeInfos": [],
            }
 
        response = _request_with_retry(
            "GET", url, headers=self.auth.get_headers(), dry_run_payload=_simulate
        )
        describe_result = response.json()
        logger.info(
            f"Described {object_name}: {len(describe_result.get('fields', []))} fields, "
            f"{len(describe_result.get('childRelationships', []))} child relationships"
        )
        return describe_result
 
    def get_record_count(self, object_name: str) -> int:
        """Get the approximate record count for an object via SOQL.
 
        Uses SELECT COUNT() FROM {ObjectName} — returns an integer.
        Note: For large objects, consider using /limits/ endpoint instead.
        """
        # Validate the object name (SOQL injection guard) before it is
        # interpolated into the COUNT() query, then issue the request through
        # the retry/timeout wrapper.
        _validate_identifier(object_name)
        query = f"SELECT COUNT() FROM {object_name}"
        url = f"{self.config.base_api_url}/query/?q={urllib.parse.quote(query)}"
 
        simulated_counts = {
            "Account": 12500, "Contact": 34200, "Opportunity": 8750,
            "Lead": 15600, "Case": 22100, "Campaign": 340,
            "Order": 5200, "Product2": 890, "Pricebook2": 12, "Contract": 1100,
        }
        response = _request_with_retry(
            "GET", url, headers=self.auth.get_headers(),
            dry_run_payload=lambda: {"totalSize": simulated_counts.get(object_name, 0)},
        )
        return response.json().get("totalSize", 0)
 
    def _get_simulated_fields(self, object_name: str) -> list:
        """Return simulated field metadata for dry-run mode."""
        common_fields = [
            {"name": "Id", "label": "Record ID", "type": "id", "length": 18,
             "nillable": False, "unique": True, "externalId": False, "referenceTo": []},
            {"name": "Name", "label": "Name", "type": "string", "length": 255,
             "nillable": True, "unique": False, "externalId": False, "referenceTo": []},
            {"name": "CreatedDate", "label": "Created Date", "type": "datetime", "length": 0,
             "nillable": False, "unique": False, "externalId": False, "referenceTo": []},
            {"name": "LastModifiedDate", "label": "Last Modified Date", "type": "datetime", "length": 0,
             "nillable": False, "unique": False, "externalId": False, "referenceTo": []},
            {"name": "OwnerId", "label": "Owner ID", "type": "reference", "length": 18,
             "nillable": False, "unique": False, "externalId": False, "referenceTo": ["User"]},
        ]
 
        object_specific_fields = {
            "Account": [
                {"name": "Industry", "label": "Industry", "type": "picklist", "length": 255,
                 "nillable": True, "unique": False, "externalId": False, "referenceTo": []},
                {"name": "AnnualRevenue", "label": "Annual Revenue", "type": "currency", "length": 0,
                 "nillable": True, "unique": False, "externalId": False, "referenceTo": []},
                {"name": "Phone", "label": "Phone", "type": "phone", "length": 40,
                 "nillable": True, "unique": False, "externalId": False, "referenceTo": []},
                {"name": "Website", "label": "Website", "type": "url", "length": 255,
                 "nillable": True, "unique": False, "externalId": False, "referenceTo": []},
                {"name": "BillingCity", "label": "Billing City", "type": "string", "length": 40,
                 "nillable": True, "unique": False, "externalId": False, "referenceTo": []},
            ],
            "Contact": [
                {"name": "FirstName", "label": "First Name", "type": "string", "length": 40,
                 "nillable": True, "unique": False, "externalId": False, "referenceTo": []},
                {"name": "LastName", "label": "Last Name", "type": "string", "length": 80,
                 "nillable": False, "unique": False, "externalId": False, "referenceTo": []},
                {"name": "Email", "label": "Email", "type": "email", "length": 80,
                 "nillable": True, "unique": False, "externalId": False, "referenceTo": []},
                {"name": "AccountId", "label": "Account ID", "type": "reference", "length": 18,
                 "nillable": True, "unique": False, "externalId": False, "referenceTo": ["Account"]},
            ],
            "Opportunity": [
                {"name": "Amount", "label": "Amount", "type": "currency", "length": 0,
                 "nillable": True, "unique": False, "externalId": False, "referenceTo": []},
                {"name": "StageName", "label": "Stage", "type": "picklist", "length": 255,
                 "nillable": False, "unique": False, "externalId": False, "referenceTo": []},
                {"name": "CloseDate", "label": "Close Date", "type": "date", "length": 0,
                 "nillable": False, "unique": False, "externalId": False, "referenceTo": []},
                {"name": "AccountId", "label": "Account ID", "type": "reference", "length": 18,
                 "nillable": True, "unique": False, "externalId": False, "referenceTo": ["Account"]},
            ],
        }
 
        extra = object_specific_fields.get(object_name, [])
        return common_fields + extra
 
 
# =============================================================================
# 4. PURVIEW TYPE DEFINITION SERVICE
# =============================================================================
 
class TypeDefService:
    """Manages custom type definitions in Purview for Salesforce assets.
 
    These types define the structure of Salesforce entities in Purview's catalog.
    Types are registered once and reused across all scans.
    """
 
    SALESFORCE_TYPES = {
        "entityDefs": [
            {
                "category": "ENTITY",
                "name": "custom_salesforce_org",
                "description": "A Salesforce organization (instance)",
                "superTypes": ["Server"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "orgId",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                        "description": "Salesforce Organization ID (18-char)",
                    },
                    {
                        "name": "instanceUrl",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                        "description": "Salesforce instance URL",
                    },
                    {
                        "name": "apiVersion",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                    {
                        "name": "environment",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                        "description": "Environment: production, sandbox, developer",
                    },
                ],
            },
            {
                "category": "ENTITY",
                "name": "custom_salesforce_object",
                "description": "A Salesforce sObject (standard or custom)",
                "superTypes": ["DataSet"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "apiName",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                        "description": "Salesforce API name (e.g., Account, Custom_Object__c)",
                    },
                    {
                        "name": "isCustom",
                        "typeName": "boolean",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                        "description": "Whether this is a custom object (ends with __c)",
                    },
                    {
                        "name": "recordCount",
                        "typeName": "long",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                    {
                        "name": "keyPrefix",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                        "description": "3-character ID prefix for this object type",
                    },
                    {
                        "name": "isQueryable",
                        "typeName": "boolean",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                    },
                ],
            },
            {
                "category": "ENTITY",
                "name": "custom_salesforce_field",
                "description": "A field on a Salesforce sObject",
                "superTypes": ["DataSet"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "fieldType",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                        "description": "Salesforce field type (string, picklist, reference, currency, etc.)",
                    },
                    {
                        "name": "fieldLength",
                        "typeName": "int",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                    },
                    {
                        "name": "isNillable",
                        "typeName": "boolean",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                    },
                    {
                        "name": "isUnique",
                        "typeName": "boolean",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                    },
                    {
                        "name": "referenceTo",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                        "description": "For reference (lookup) fields: the target object API name",
                    },
                ],
            },
            {
                "category": "ENTITY",
                "name": "custom_salesforce_process",
                "description": "A data movement process from Salesforce to another system",
                "superTypes": ["Process"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "processType",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                        "description": "Type of process: ETL, API Sync, Streaming, etc.",
                    },
                    {
                        "name": "schedule",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                        "description": "Execution schedule (e.g., Daily 2:00 AM UTC)",
                    },
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
    for _t in SALESFORCE_TYPES["entityDefs"]:
        _t["attributeDefs"].append(dict(_SCAN_RUN_ATTR))
        _t["attributeDefs"].append(dict(_SOURCE_OF_TRUTH_ATTR))
    del _t
 
    @staticmethod
    def register_types(purview_endpoint: str, bearer_token: str) -> dict:
        """Register Salesforce custom types in Purview.
 
        Uses: POST {endpoint}/api/atlas/v2/types/typedefs
 
        Types are idempotent — re-registering updates existing types.
        """
        url = f"{purview_endpoint}/api/atlas/v2/types/typedefs"
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
        }
 
        type_names = [t["name"] for t in TypeDefService.SALESFORCE_TYPES["entityDefs"]]
        # Idempotent registration through the retry/timeout wrapper (re-running
        # updates existing types). In dry-run the wrapper echoes the type set.
        _request_with_retry(
            "POST", url, json=TypeDefService.SALESFORCE_TYPES, headers=headers,
            dry_run_payload=lambda: TypeDefService.SALESFORCE_TYPES,
        )
        logger.info(f"Registered {len(type_names)} Salesforce types: {type_names}")
        return TypeDefService.SALESFORCE_TYPES
 
 
# =============================================================================
# 5. ENTITY SERVICE
# =============================================================================
 
class EntityService:
    """Creates and manages entities in Purview via Atlas v2 REST API."""
 
    BATCH_SIZE = 50  # Max entities per bulk API call
 
    @staticmethod
    def build_entity(
        type_name: str,
        qualified_name: str,
        name: str,
        description: str = "",
        attributes: dict = None,
        classifications: list = None,
    ) -> dict:
        """Build an Atlas v2 entity dict.
 
        Args:
            type_name: The custom type name (e.g., "custom_salesforce_object")
            qualified_name: Globally unique identifier for the entity
            name: Human-readable display name
            description: Optional description
            attributes: Additional type-specific attributes
 
        Returns:
            Atlas v2 entity dict ready for the /entity/bulk endpoint.
        """
        entity = {
            "typeName": type_name,
            "attributes": {
                "qualifiedName": qualified_name,
                # M1: names/descriptions are untrusted source metadata
                "name": _sanitize_text(name, MAX_NAME_COMPONENT_LENGTH),
                "description": _sanitize_text(description),
                "scanRunId": SCAN_RUN_ID,  # rollback support
                "sourceOfTruth": SOURCE_OF_TRUTH,  # provenance (Security Review 8.2)
                **(attributes or {}),
            },
            "status": "ACTIVE",
        }
        # Classifications derive from the shared ClassificationEngine
        # (classification_rules.json) and ride on the entity at creation.
        if classifications:
            entity["classifications"] = [{"typeName": c} for c in classifications]
        return entity
 
    @staticmethod
    def create_entities_bulk(
        purview_endpoint: str,
        bearer_token: str,
        entities: list,
    ) -> dict:
        """Push entities to Purview via the bulk entity API.
 
        Uses: POST {endpoint}/api/atlas/v2/entity/bulk
        Supports up to 50 entities per request. Automatically batches.
        Entities are upserted based on qualifiedName (safe to re-run).
        """
        url = f"{purview_endpoint}/api/atlas/v2/entity/bulk"
        results = []
 
        for i in range(0, len(entities), EntityService.BATCH_SIZE):
            batch = entities[i : i + EntityService.BATCH_SIZE]
            payload = {"entities": batch}
 
            headers = {
                "Authorization": f"Bearer {bearer_token}",
                "Content-Type": "application/json",
            }
 
            batch_names = [e["attributes"]["qualifiedName"] for e in batch]
            # Push the batch through the retry/timeout wrapper (entities are
            # upserted by qualifiedName — safe to re-run). In dry-run the
            # wrapper returns a synthetic mutation result.
            result = _request_with_retry(
                "POST", url, json=payload, headers=headers,
                dry_run_payload=lambda names=batch_names: {"mutatedEntities": {}, "qualifiedNames": names},
            )
            results.append(result.json())
            logger.info(f"POSTed batch of {len(batch)} entities to {url}")
            for name in batch_names:
                logger.info(f"  → {name}")
 
        return {"batches_sent": (len(entities) + EntityService.BATCH_SIZE - 1) // EntityService.BATCH_SIZE}
 
 
# =============================================================================
# 6. LINEAGE SERVICE
# =============================================================================
 
class LineageService:
    """Creates lineage (data flow) relationships in Purview.
 
    Lineage is modeled as a Process entity with inputs and outputs:
        [Source A] ──┐
                     ├──▶ [Process] ──▶ [Destination C]
        [Source B] ──┘
    """
 
    @staticmethod
    def build_process_entity(
        qualified_name: str,
        name: str,
        process_type: str,
        input_qualified_names: list,
        input_type_names: list,
        output_qualified_names: list,
        output_type_names: list,
        description: str = "",
        schedule: str = "",
    ) -> dict:
        """Build a Process entity that represents data flow (lineage).
 
        Args:
            qualified_name: Unique name for the process
            name: Human-readable process name
            process_type: ETL, API Sync, Streaming, etc.
            input_qualified_names: List of source entity qualifiedNames
            input_type_names: List of source entity type names
            output_qualified_names: List of destination entity qualifiedNames
            output_type_names: List of destination entity type names
        """
        inputs = [
            {"typeName": type_name, "uniqueAttributes": {"qualifiedName": qn}}
            for qn, type_name in zip(input_qualified_names, input_type_names)
        ]
        outputs = [
            {"typeName": type_name, "uniqueAttributes": {"qualifiedName": qn}}
            for qn, type_name in zip(output_qualified_names, output_type_names)
        ]
 
        process = {
            "typeName": "custom_salesforce_process",
            "attributes": {
                "qualifiedName": qualified_name,
                "name": name,
                "description": description,
                "processType": process_type,
                "schedule": schedule,
                "inputs": inputs,
                "outputs": outputs,
            },
            "status": "ACTIVE",
        }
        return process
 
    @staticmethod
    def create_lineage(
        purview_endpoint: str,
        bearer_token: str,
        process_entities: list,
    ) -> dict:
        """Push lineage (process) entities to Purview."""
        return EntityService.create_entities_bulk(
            purview_endpoint, bearer_token, process_entities
        )
 
 
# =============================================================================
# 7. BUSINESS METADATA SERVICE
# =============================================================================
 
class MetadataService:
    """Applies business metadata and classifications to entities in Purview."""
 
    @staticmethod
    def apply_business_metadata(
        purview_endpoint: str,
        bearer_token: str,
        entity_guid: str,
        metadata: dict,
    ) -> None:
        """Apply business metadata to an entity.
 
        Uses: POST {endpoint}/api/atlas/v2/entity/guid/{guid}/businessmetadata?isOverwrite=true
 
        Args:
            entity_guid: The GUID of the target entity
            metadata: Dict of {BusinessMetadataTypeName: {attribute: value}}
        """
        url = f"{purview_endpoint}/api/atlas/v2/entity/guid/{entity_guid}/businessmetadata?isOverwrite=true"
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
        }
 
        _request_with_retry(
            "POST", url, json=metadata, headers=headers,
            dry_run_payload=lambda: {"guid": entity_guid},
        )
        logger.info(f"Applied business metadata to entity {entity_guid}: {metadata}")
 
    @staticmethod
    def apply_classification(
        purview_endpoint: str,
        bearer_token: str,
        entity_guid: str,
        classification_name: str,
    ) -> None:
        """Apply a classification (tag) to an entity.
 
        Uses: POST {endpoint}/api/atlas/v2/entity/guid/{guid}/classifications
        """
        url = f"{purview_endpoint}/api/atlas/v2/entity/guid/{entity_guid}/classifications"
        payload = [{"typeName": classification_name}]
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
        }
 
        _request_with_retry(
            "POST", url, json=payload, headers=headers,
            dry_run_payload=lambda: {"guid": entity_guid},
        )
        logger.info(f"Applied classification '{classification_name}' to entity {entity_guid}")
 
 
# =============================================================================
# 8. SALESFORCE CONNECTOR (ORCHESTRATOR)
# =============================================================================
 
class SalesforceConnector:
    """Main connector that orchestrates the full Salesforce → Purview flow.
 
    This is the equivalent of the SQL Server connector in the other example,
    but targets Salesforce as the data source. The Purview-side logic
    (type registration, entity creation, lineage, metadata) is identical.
    """
 
    def __init__(
        self,
        purview_config: PurviewConfig,
        salesforce_config: SalesforceConfig,
        sf_org_name: str = "mycompany",
    ):
        self.purview_config = purview_config
        self.sf_config = salesforce_config
        self.sf_org_name = sf_org_name
 
        # Initialize services
        self.purview_auth = PurviewAuthService(purview_config)
        self.sf_auth = SalesforceAuthService(salesforce_config)
        self.discovery = SalesforceDiscoveryService(salesforce_config, self.sf_auth)
 
    def run(self, objects_to_scan: list = None, lineage_mappings: list = None):
        """Execute the full connector pipeline.
 
        Steps:
        1. Authenticate to both Purview and Salesforce
        2. Register custom Salesforce types in Purview
        3. Discover Salesforce objects and fields
        4. Create entities in Purview (org → objects → fields)
        5. Build cross-system lineage
        6. Apply business metadata and classifications
        """
        objects_to_scan = objects_to_scan or OBJECTS_TO_SCAN
        lineage_mappings = lineage_mappings or LINEAGE_MAPPINGS
 
        logger.info("=" * 70)
        logger.info("SALESFORCE → PURVIEW CUSTOM CONNECTOR")
        logger.info("=" * 70)
 
        # --- Step 1: Authenticate ---
        logger.info("\n--- Step 1: Authentication ---")
        purview_token = self.purview_auth.get_bearer_token()
        self.sf_auth.authenticate()
        purview_endpoint = self.purview_config.endpoint
 
        # --- Step 2: Register custom types ---
        logger.info("\n--- Step 2: Register Salesforce Custom Types in Purview ---")
        TypeDefService.register_types(purview_endpoint, purview_token)
 
        # --- Step 3: Discover Salesforce metadata ---
        logger.info("\n--- Step 3: Discover Salesforce Metadata ---")
        sf_objects = self.discovery.discover_objects(objects_to_scan)
        # LLM10: cap discovery breadth (env-configurable; skips are logged)
        sf_objects = _apply_cap(sf_objects, MAX_OBJECTS_PER_RUN, "Salesforce objects")
 
        # Classifications are driven by classification_rules.json via the
        # shared ClassificationEngine — no hardcoded classification logic.
        classification_engine = ClassificationEngine()
        logger.info(
            f"Classification engine loaded: "
            f"{classification_engine.get_stats()['total']} rules"
        )
 
        all_entities = []
        object_details = {}  # Store describe results for later use
 
        # Create the org-level entity
        org_qualified_name = f"salesforce://{self.sf_org_name}"
        org_entity = EntityService.build_entity(
            type_name="custom_salesforce_org",
            qualified_name=org_qualified_name,
            name=f"Salesforce - {self.sf_org_name}",
            description=f"Salesforce organization: {self.sf_org_name}",
            attributes={
                "instanceUrl": self.sf_config.instance_url or self.sf_config.domain_url,
                "apiVersion": self.sf_config.api_version,
                "environment": "production",
            },
        )
        all_entities.append(org_entity)
 
        # Discover and create entities for each object
        for obj in sf_objects:
            obj_name = _safe_name_component(obj["name"])  # M1: untrusted source metadata
            obj_label = obj["label"]
 
            # Describe the object to get field-level metadata. Pass the scan's
            # object list as the allow-list so describe_object rejects any name
            # outside the requested set (None in scan-all mode).
            describe_result = self.discovery.describe_object(obj_name, objects_to_scan)
            object_details[obj_name] = describe_result
 
            # Get record count (enrichment beyond what native connector provides)
            record_count = self.discovery.get_record_count(obj_name)
 
            # Build the object entity
            obj_qualified_name = f"salesforce://{self.sf_org_name}/{obj_name}"
            obj_entity = EntityService.build_entity(
                type_name="custom_salesforce_object",
                qualified_name=obj_qualified_name,
                name=obj_label,
                description=f"Salesforce object: {obj_label} ({obj_name})",
                attributes={
                    "apiName": obj_name,
                    "isCustom": obj.get("custom", False),
                    "recordCount": record_count,
                    "keyPrefix": obj.get("keyPrefix", ""),
                    "isQueryable": obj.get("queryable", True),
                },
            )
            all_entities.append(obj_entity)
 
            # Build field entities
            # LLM10: cap per-object field breadth
            capped_fields = _apply_cap(
                describe_result.get("fields", []),
                MAX_FIELDS_PER_OBJECT, f"fields on {obj_name}",
            )
            for fld in capped_fields:
                fld_name = _safe_name_component(fld["name"])  # M1
                fld_qualified_name = f"salesforce://{self.sf_org_name}/{obj_name}/{fld_name}"
 
                ref_to = fld.get("referenceTo", [])
                ref_to_str = ref_to[0] if ref_to else ""
 
                fld_classification = classification_engine.classify_field(
                    source="salesforce", object_name=obj_name,
                    field_name=fld_name, field_type=fld.get("type"),
                )
                fld_entity = EntityService.build_entity(
                    type_name="custom_salesforce_field",
                    qualified_name=fld_qualified_name,
                    name=fld.get("label", fld_name),
                    description=f"Field {fld_name} on {obj_name} (type: {fld['type']})",
                    attributes={
                        "fieldType": fld["type"],
                        "fieldLength": fld.get("length", 0),
                        "isNillable": fld.get("nillable", True),
                        "isUnique": fld.get("unique", False),
                        "referenceTo": ref_to_str,
                    },
                    classifications=[fld_classification] if fld_classification else None,
                )
                all_entities.append(fld_entity)
 
        # LLM04: warn if asset descriptions drifted since the last run
        _check_metadata_drift(
            (e["attributes"]["qualifiedName"], e["attributes"].get("description", ""))
            for e in all_entities
        )
 
        # --- Step 4: Push entities to Purview ---
        logger.info(f"\n--- Step 4: Create {len(all_entities)} Entities in Purview ---")
        EntityService.create_entities_bulk(purview_endpoint, purview_token, all_entities)
 
        # --- Step 5: Build cross-system lineage ---
        logger.info("\n--- Step 5: Build Cross-System Lineage ---")
        process_entities = []
 
        for mapping in lineage_mappings:
            source_qns = [
                f"salesforce://{self.sf_org_name}/{obj}"
                for obj in mapping["source_objects"]
            ]
            source_types = ["custom_salesforce_object"] * len(mapping["source_objects"])
 
            process_qn = (
                f"salesforce://{self.sf_org_name}/process/"
                f"{mapping['process_name'].replace(' ', '_').lower()}"
            )
 
            process_entity = LineageService.build_process_entity(
                qualified_name=process_qn,
                name=mapping["process_name"],
                process_type=mapping["process_type"],
                input_qualified_names=source_qns,
                input_type_names=source_types,
                output_qualified_names=[mapping["destination_table"]],
                output_type_names=[mapping["destination_type"]],
                description=mapping.get("description", ""),
                schedule="Daily 2:00 AM UTC",
            )
            process_entities.append(process_entity)
 
            logger.info(
                f"  Lineage: {mapping['source_objects']} → "
                f"{mapping['process_name']} → {mapping['destination_table']}"
            )
 
        LineageService.create_lineage(purview_endpoint, purview_token, process_entities)
 
        # --- Step 6: Apply business metadata and classifications ---
        logger.info("\n--- Step 6: Apply Business Metadata and Classifications ---")
 
        # In a real implementation, you would:
        # 1. Query Purview for the GUID of each entity by qualifiedName
        # 2. Apply business metadata using the GUID
        # 3. Apply classifications using the GUID
        #
        # Example (uncomment for real usage):
        # search_url = f"{purview_endpoint}/api/atlas/v2/search/advanced"
        # search_payload = {
        #     "keywords": None,
        #     "filter": {
        #         "and": [
        #             {"attributeName": "qualifiedName", "operator": "eq",
        #              "attributeValue": "salesforce://mycompany/Account"}
        #         ]
        #     }
        # }
        # response = requests.post(search_url, json=search_payload, headers=headers)
        # guid = response.json()["value"][0]["id"]
 
        sample_guid = "dry-run-guid-account-001"
        if APPLY_BUSINESS_METADATA:
            # ⚠️ M3 PLACEHOLDER VALUES: replace with values computed from real
            # sources (data-quality jobs, ownership registries) before enabling
            # the flag. Fabricated governance signals mislead both humans and AI
            # features that treat the catalog as authoritative.
            MetadataService.apply_business_metadata(
                purview_endpoint,
                purview_token,
                entity_guid=sample_guid,
                metadata={
                    "DataQuality": {
                        "lastValidated": "2026-02-16T02:00:00Z",
                        "qualityScore": 94.5,
                        "dataOwner": "CRM Team",
                        "dataSteward": "Jane Smith",
                    }
                },
            )
        else:
            logger.info(
                "Skipping business metadata (APPLY_BUSINESS_METADATA is not 'true'). "
                "Replace the placeholder values with values computed from real "
                "sources (data-quality jobs, ownership registries) before enabling."
            )
 
        # Classifications were attached at entity-build time by the shared
        # ClassificationEngine (classification_rules.json) — no hardcoded
        # PII/financial field lists. Log what the engine classified.
        classified_fields = [e for e in all_entities if e.get("classifications")]
        for e in classified_fields:
            logger.info(
                f"  Classified {e['attributes']['qualifiedName']} -> "
                f"{[c['typeName'] for c in e['classifications']]}"
            )
 
        # --- Summary ---
        logger.info("\n" + "=" * 70)
        logger.info("CONNECTOR RUN COMPLETE")
        logger.info("=" * 70)
        logger.info(f"  Org entity:       1")
        logger.info(f"  Object entities:  {len(sf_objects)}")
        field_count = sum(
            len(object_details[obj["name"]].get("fields", []))
            for obj in sf_objects
        )
        logger.info(f"  Field entities:   {field_count}")
        logger.info(f"  Process entities: {len(process_entities)}")
        logger.info(f"  Total entities:   {len(all_entities) + len(process_entities)}")
        logger.info(f"  Fields classified (engine): {len(classified_fields)}")
 
 
# =============================================================================
# 9. MAIN ENTRY POINT
# =============================================================================
 
def main():
    """Run the Salesforce → Purview custom connector.
 
    In production (Azure Functions), this would be called by a Timer or HTTP trigger.
    Credentials would come from Managed Identity + Key Vault.
    """
    logger.info("Initializing Salesforce → Purview custom connector...")
 
    # Load Purview configuration
    purview_config = PurviewConfig.from_environment()
 
    # Load Salesforce configuration from Key Vault
    kv_url = os.environ.get("KEY_VAULT_URL", "https://my-keyvault.vault.azure.net/")
    sf_config = SalesforceConfig.from_key_vault(kv_url)
 
    # Create and run the connector
    connector = SalesforceConnector(
        purview_config=purview_config,
        salesforce_config=sf_config,
        sf_org_name="mycompany",  # Used in qualifiedName: salesforce://mycompany/...
    )
 
    connector.run(
        objects_to_scan=OBJECTS_TO_SCAN,
        lineage_mappings=LINEAGE_MAPPINGS,
    )
 
 
if __name__ == "__main__":
    main()
 
