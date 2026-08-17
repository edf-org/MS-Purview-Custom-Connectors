"""
Microsoft Purview Custom Connector - Example Implementation
============================================================
 
This example demonstrates a complete custom connector for SQL Server that:
1. Authenticates to Purview using a Service Principal
2. Registers custom type definitions
3. Creates entities (database → schema → table → column hierarchy)
4. Builds lineage between source and destination tables
5. Applies business metadata and classifications
 
Prerequisites:
    pip install pyapacheatlas azure-identity python-dotenv requests
 
   PRODUCTION DEPENDENCY PINNING (L4, supply chain): floor pins (>=) are fine
   for development, but production deployments should install from a
   hash-pinned lock file so a compromised or hijacked package version cannot
   be silently pulled in:
       pip install pip-tools
       pip-compile --generate-hashes requirements.in -o requirements.txt
       pip install --require-hashes -r requirements.txt
   Re-generate the lock file (and review the diff) as part of dependency
   updates in CI, and run an SCA scan against it.
 
Environment Variables (set in .env file or system environment):
    AZURE_TENANT_ID       - Azure AD tenant ID
    AZURE_CLIENT_ID       - Service Principal application ID
    AZURE_CLIENT_SECRET   - Service Principal client secret
    PURVIEW_ACCOUNT_NAME  - Purview account name (without .purview.azure.com)
 
Purview role assignment — LEAST PRIVILEGE (LLM06 hardening, see Security
Review): assign the "Data Curator" and "Data Source Administrator" roles to
the Service Principal (or Managed Identity) on a DEDICATED COLLECTION for
this source rather than the root collection, so the connector identity can
only write within its own scope. Use ONE identity per connector (not a
shared identity across connectors) so blast radius and audit trails stay
separable.
 
Usage:
    python purview_sql_custom_connector_example.py
 
Classification:
    Column classifications are driven by classification_rules.json via the
    shared ClassificationEngine (classification_engine.py). Data stewards
    maintain the rules file — no Python changes are needed to add, modify,
    or disable a classification rule.
"""
 
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional
 
# Shared, configuration-driven classification engine (rules live in
# classification_rules.json — maintained by data stewards)
from classification_engine import ClassificationEngine
 
# --- Uncomment these imports when running against a real Purview account ---
# from pyapacheatlas.auth import ServicePrincipalAuthentication
# from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess
# from pyapacheatlas.core.typedef import (
#     AtlasAttributeDef,
#     EntityTypeDef,
#     RelationshipTypeDef,
# )
# from azure.identity import DefaultAzureCredential
# from dotenv import load_dotenv
 
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
 
# --- Security: default timeout for all HTTP requests (seconds) ---
# Tuple format: (connect_timeout, read_timeout). Applied by _request_with_retry
# to every Purview request.
REQUEST_TIMEOUT = (10, 30)

# Dry-run transport toggle. When true (the default for these examples), HTTP
# calls are simulated by _DryRunResponse *through the same request path* used in
# live mode — so identifier validation, the timeout, the bearer token, and the
# retry/backoff wrapper are all genuinely exercised without real credentials.
# Live mode: set CONNECTOR_DRY_RUN=false and uncomment `import requests`.
DRY_RUN = os.environ.get("CONNECTOR_DRY_RUN", "true").strip().lower() != "false"
 
def _validate_identifier(value: str, allow_list: list = None) -> str:
    """Validate that a string is a safe SQL/API identifier."""
    if allow_list and value not in allow_list:
        raise ValueError(f"Identifier '{value}' is not in the allow-list.")
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', value):
        raise ValueError(f"Identifier '{value}' contains invalid characters.")
    return value
 
 
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

    Lets the full request path — bearer token, the configured timeout, and the
    retry/backoff wrapper — execute unchanged without a live endpoint or the
    `requests` dependency. status_code is 200 so the wrapper's success path
    runs; json() returns the simulated payload.
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
 
    Retries on 429 (honoring Retry-After), transient 5xx, and connection or
    timeout errors. In live mode (DRY_RUN false) it calls requests.request; in
    dry-run it returns a _DryRunResponse from `dry_run_payload` (value or
    zero-arg callable), exercising the same timed-out, retry-wrapped path.
 
    Usage (replaces bare requests.get/post calls):
        response = _request_with_retry("POST", url, json=payload,
                                       headers=headers, dry_run_payload=lambda: {...})
    """
    import time
    import random
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)
    for attempt in range(MAX_RETRIES + 1):
        try:
            if DRY_RUN:
                # Simulated transport: exercises the bearer token, timeout, and
                # this retry/backoff wrapper without a live endpoint. The timeout
                # that would be sent is logged and carried on the response.
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
# 0. SCAN RUN IDENTIFIER & PROVENANCE (rollback support / Security Review 8.2)
# =============================================================================
# Every entity written by this run is stamped with a scan run ID via the
# custom "scanRunId" attribute. If a run goes bad, the standalone utility
# purview_rollback_scan_run.py can locate everything stamped with a given
# run ID and soft-delete it. The ID is logged at startup so it is captured
# in Application Insights — always note it before/after a production run.
 
SCAN_RUN_ID = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
logger.info(f"Scan run ID: {SCAN_RUN_ID}")
 
# Provenance tagging (Security Review 8.2): every connector-written entity is
# marked so downstream consumers — human curators and AI features alike — can
# distinguish machine-synced text from human-curated text.
SOURCE_OF_TRUTH = "connector:sql"
 
 
# =============================================================================
# 1. CONFIGURATION
# =============================================================================
 
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
        """Purview Data Map endpoint base URL.
 
        Defaults to the classic account endpoint. If your tenant has been
        upgraded to the new unified Microsoft Purview portal
        (purview.microsoft.com), set the PURVIEW_ENDPOINT environment
        variable to override, e.g.:
            PURVIEW_ENDPOINT=https://api.purview-service.microsoft.com/datamap
        The Atlas v2 API paths are identical on both endpoints.
        """
        override = os.environ.get("PURVIEW_ENDPOINT", "").rstrip("/")
        if override:
            return _validate_purview_endpoint(override)  # L1: allow-list check
        return f"https://{self.account_name}.purview.azure.com/datamap"
 
 
# =============================================================================
# 2. AUTHENTICATION SERVICE
# =============================================================================
 
class AuthService:
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
        logger.info("[DRY RUN] Would acquire bearer token via DefaultAzureCredential")
        return "dry-run-token"
 
 
# =============================================================================
# 3. TYPE DEFINITION SERVICE
# =============================================================================
 
class TypeDefService:
    """Manages custom type definitions in Purview.
 
    Custom types must be registered before creating entities of that type.
    Types are idempotent — re-registering an existing type updates it.
    """
 
    # Example custom type definitions for a SQL-like source
    CUSTOM_TYPES = {
        "entityDefs": [
            {
                "category": "ENTITY",
                "name": "custom_sql_server",
                "description": "A custom SQL Server instance",
                "superTypes": ["Server"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "serverVersion",
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
                        "description": "Environment: dev, staging, production",
                    },
                ],
            },
            {
                "category": "ENTITY",
                "name": "custom_sql_database",
                "description": "A database within a custom SQL Server",
                "superTypes": ["DataSet"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "databaseEngine",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                ],
            },
            {
                "category": "ENTITY",
                "name": "custom_sql_table",
                "description": "A table within a custom SQL database",
                "superTypes": ["DataSet"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "rowCount",
                        "typeName": "long",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                    {
                        "name": "schemaName",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                ],
            },
            {
                "category": "ENTITY",
                "name": "custom_sql_column",
                "description": "A column within a custom SQL table",
                "superTypes": ["DataSet"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "dataType",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                    {
                        "name": "isNullable",
                        "typeName": "boolean",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                    },
                    {
                        "name": "isPrimaryKey",
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
                "name": "custom_sql_process",
                "description": "A data transformation process (ETL, stored proc, etc.)",
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
                        "description": "Type of process: ETL, StoredProcedure, View, etc.",
                    },
                    {
                        "name": "queryText",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                    },
                ],
            },
        ]
    }
 
    # Rollback + provenance support: stamp every custom type with searchable
    # scanRunId (used by purview_rollback_scan_run.py) and sourceOfTruth
    # (provenance marker, Security Review 8.2) attributes.
    _SCAN_RUN_ATTR = {
        "name": "scanRunId", "typeName": "string", "isOptional": True,
        "cardinality": "SINGLE", "isUnique": False, "isIndexable": True,
        "description": "ID of the scan run that last wrote this entity",
    }
    _SOURCE_OF_TRUTH_ATTR = {
        "name": "sourceOfTruth", "typeName": "string", "isOptional": True,
        "cardinality": "SINGLE", "isUnique": False, "isIndexable": True,
        "description": "Provenance marker: which connector wrote this entity",
    }
    for _t in CUSTOM_TYPES["entityDefs"]:
        _t["attributeDefs"].append(dict(_SCAN_RUN_ATTR))
        _t["attributeDefs"].append(dict(_SOURCE_OF_TRUTH_ATTR))
    del _t
 
    def __init__(self, client, config: PurviewConfig, auth: AuthService):
        self.client = client
        self.config = config
        self.auth = auth
 
    def register_types(self) -> dict:
        """Register all custom type definitions in Purview.
 
        Types are idempotent — if they already exist, they are updated.
        """
        logger.info("Registering custom type definitions...")
 
        # Compose the real call path: bearer token -> validated endpoint ->
        # retry/timeout wrapper. In dry-run the wrapper echoes CUSTOM_TYPES.
        token = self.auth.get_bearer_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        url = f"{self.config.endpoint}/api/atlas/v2/types/typedefs"
        _request_with_retry(
            "POST", url, headers=headers, json=self.CUSTOM_TYPES,
            dry_run_payload=lambda: self.CUSTOM_TYPES,
        )
        logger.info(f"Registered {len(self.CUSTOM_TYPES['entityDefs'])} type definitions:")
        for td in self.CUSTOM_TYPES["entityDefs"]:
            logger.info(f"  - {td['name']} (superType: {td['superTypes'][0]})")
        return self.CUSTOM_TYPES
 
 
# =============================================================================
# 4. ENTITY SERVICE
# =============================================================================
 
@dataclass
class SourceAsset:
    """Represents a discovered asset from the source system."""
    name: str
    qualified_name: str
    entity_type: str
    attributes: dict = field(default_factory=dict)
    classifications: list = field(default_factory=list)
 
 
class EntityService:
    """Creates and manages entities in Purview.
 
    Uses the bulk entity API for efficient batch creation.
    Entities are upserted based on qualifiedName.
    """
 
    BATCH_SIZE = 50  # Max entities per bulk API call
 
    def __init__(self, client, config: PurviewConfig, auth: AuthService):
        self.client = client
        self.config = config
        self.auth = auth
 
    def build_entity(self, asset: SourceAsset) -> dict:
        """Convert a SourceAsset into an Atlas entity payload."""
        entity = {
            "typeName": asset.entity_type,
            "attributes": {
                "qualifiedName": asset.qualified_name,
                # M1: source-derived name is untrusted metadata
                "name": _sanitize_text(asset.name, MAX_NAME_COMPONENT_LENGTH),
                "scanRunId": SCAN_RUN_ID,  # rollback support
                "sourceOfTruth": SOURCE_OF_TRUTH,  # provenance (Security Review 8.2)
                **asset.attributes,
            },
            "status": "ACTIVE",
        }
        if asset.classifications:
            entity["classifications"] = [
                {"typeName": c} for c in asset.classifications
            ]
        return entity
 
    def create_entities_bulk(self, assets: list[SourceAsset]) -> list[dict]:
        """Create or update entities in Purview in batches.
 
        Returns list of API responses (one per batch).
        """
        results = []
        for i in range(0, len(assets), self.BATCH_SIZE):
            batch = assets[i : i + self.BATCH_SIZE]
            entities = [self.build_entity(a) for a in batch]
            payload = {"entities": entities}
 
            logger.info(f"Creating batch of {len(entities)} entities (batch {i // self.BATCH_SIZE + 1})...")
 
            # --- Alternative using pyapacheatlas ---
            # atlas_entities = [
            #     AtlasEntity(
            #         name=a.name,
            #         typeName=a.entity_type,
            #         qualified_name=a.qualified_name,
            #         attributes=a.attributes,
            #     )
            #     for a in batch
            # ]
            # result = self.client.upload_entities(atlas_entities)
            # results.append(result)
 
            # Compose the real call path: bearer token -> validated endpoint ->
            # retry/timeout wrapper. Entities upsert by qualifiedName (safe to
            # re-run). In dry-run the wrapper echoes the batch payload.
            token = self.auth.get_bearer_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            url = f"{self.config.endpoint}/api/atlas/v2/entity/bulk"
            response = _request_with_retry(
                "POST", url, headers=headers, json=payload,
                dry_run_payload=lambda p=payload: p,
            )
            for e in entities:
                logger.info(f"  - [{e['typeName']}] {e['attributes']['qualifiedName']}")
            results.append(response.json())
 
        return results
 
 
# =============================================================================
# 5. LINEAGE SERVICE
# =============================================================================
 
@dataclass
class LineageRelationship:
    """Defines a lineage relationship: inputs → process → outputs."""
    process_name: str
    process_qualified_name: str
    process_type: str  # e.g., "ETL", "StoredProcedure"
    input_qualified_names: list[str]
    output_qualified_names: list[str]
    input_type: str = "custom_sql_table"
    output_type: str = "custom_sql_table"
    query_text: Optional[str] = None
 
 
class LineageService:
    """Creates lineage relationships in Purview.
 
    Lineage is modeled as a Process entity with inputs and outputs.
    Each Process entity connects source entities to destination entities.
    """
 
    def __init__(self, client, config: PurviewConfig, auth: AuthService):
        self.client = client
        self.config = config
        self.auth = auth
 
    def create_lineage(self, relationship: LineageRelationship) -> dict:
        """Create a lineage process entity linking inputs to outputs.
 
        Uses the bulk entity API with a Process-typed entity that references
        input and output entities by their qualifiedName.
        """
        logger.info(
            f"Creating lineage: {len(relationship.input_qualified_names)} inputs → "
            f"[{relationship.process_name}] → {len(relationship.output_qualified_names)} outputs"
        )
 
        # Build the process entity with input/output references
        process_entity = {
            "typeName": "custom_sql_process",
            "attributes": {
                "qualifiedName": relationship.process_qualified_name,
                "name": relationship.process_name,
                "processType": relationship.process_type,
                "inputs": [
                    {
                        "typeName": relationship.input_type,
                        "uniqueAttributes": {"qualifiedName": qn},
                    }
                    for qn in relationship.input_qualified_names
                ],
                "outputs": [
                    {
                        "typeName": relationship.output_type,
                        "uniqueAttributes": {"qualifiedName": qn},
                    }
                    for qn in relationship.output_qualified_names
                ],
            },
            "status": "ACTIVE",
        }
 
        if relationship.query_text:
            process_entity["attributes"]["queryText"] = relationship.query_text
 
        payload = {"entities": [process_entity]}
 
        # --- Alternative using pyapacheatlas ---
        # process = AtlasProcess(
        #     name=relationship.process_name,
        #     typeName="custom_sql_process",
        #     qualified_name=relationship.process_qualified_name,
        #     inputs=[
        #         AtlasEntity(
        #             name=qn.split("/")[-1],
        #             typeName=relationship.input_type,
        #             qualified_name=qn,
        #         )
        #         for qn in relationship.input_qualified_names
        #     ],
        #     outputs=[
        #         AtlasEntity(
        #             name=qn.split("/")[-1],
        #             typeName=relationship.output_type,
        #             qualified_name=qn,
        #         )
        #         for qn in relationship.output_qualified_names
        #     ],
        #     attributes={"processType": relationship.process_type},
        # )
        # return self.client.upload_entities([process])
 
        # Compose the real call path: bearer token -> validated endpoint ->
        # retry/timeout wrapper. In dry-run the wrapper echoes the payload.
        token = self.auth.get_bearer_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        url = f"{self.config.endpoint}/api/atlas/v2/entity/bulk"
        response = _request_with_retry(
            "POST", url, headers=headers, json=payload,
            dry_run_payload=lambda: payload,
        )
        logger.info("Created lineage process entity:")
        logger.info(f"  Process: {relationship.process_qualified_name}")
        for qn in relationship.input_qualified_names:
            logger.info(f"  Input:   {qn}")
        for qn in relationship.output_qualified_names:
            logger.info(f"  Output:  {qn}")
        return response.json()
 
 
# =============================================================================
# 6. BUSINESS METADATA SERVICE
# =============================================================================
 
class MetadataService:
    """Manages business metadata and classifications on entities."""
 
    def __init__(self, config: PurviewConfig, auth: AuthService):
        self.config = config
        self.auth = auth

    def apply_business_metadata(self, entity_guid: str, metadata: dict) -> dict:
        """Apply business metadata key-value pairs to an entity.
 
        Args:
            entity_guid: The GUID of the target entity.
            metadata: Dict of {business_metadata_name: {attr_name: attr_value}}.
 
        Example metadata:
            {
                "DataQuality": {
                    "lastValidated": "2026-01-15",
                    "qualityScore": 95,
                    "dataOwner": "analytics-team@company.com"
                }
            }
        """
        logger.info(f"Applying business metadata to entity {entity_guid}...")
 
        token = self.auth.get_bearer_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        url = (f"{self.config.endpoint}/api/atlas/v2/entity/guid/{entity_guid}"
               f"/businessmetadata?isOverwrite=true")
        _request_with_retry(
            "POST", url, headers=headers, json=metadata,
            dry_run_payload=lambda: metadata,
        )
        logger.info(f"Applied business metadata to {entity_guid}:")
        logger.info(f"  {json.dumps(metadata, indent=2)}")
        return metadata
 
    def apply_classifications(self, entity_guid: str, classification_names: list[str]) -> dict:
        """Apply classification labels to an entity.
 
        Args:
            entity_guid: The GUID of the target entity.
            classification_names: List of classification type names (e.g., ["PII", "Confidential"]).
        """
        logger.info(f"Applying {len(classification_names)} classifications to entity {entity_guid}...")
 
        classifications = [{"typeName": name} for name in classification_names]
 
        token = self.auth.get_bearer_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        url = f"{self.config.endpoint}/api/atlas/v2/entity/guid/{entity_guid}/classifications"
        _request_with_retry(
            "POST", url, headers=headers, json=classifications,
            dry_run_payload=lambda: {"classifications": classifications},
        )
        logger.info(f"Applied classifications: {classification_names}")
        return {"classifications": classifications}
 
 
# =============================================================================
# 7. EXAMPLE CONNECTOR - SQL SERVER
# =============================================================================
 
class SQLServerConnector:
    """Example connector that discovers metadata from a SQL Server
    and pushes it to Purview.
 
    In a real implementation, this would query SQL Server's
    INFORMATION_SCHEMA to discover databases, schemas, tables, and columns.
    """
 
    SOURCE_TYPE = "custom_sql"
 
    def __init__(self, server_name: str, config: PurviewConfig):
        self.server_name = server_name
        self.config = config
 
    def _qualified_name(self, *parts: str) -> str:
        """Build a consistent qualifiedName for an asset.
 
        M1: every component is validated before use. In production the parts
        come from source-system metadata queries (database, schema, table,
        column names) — untrusted input that could otherwise corrupt the
        catalog hierarchy or spoof another asset's identity.
        """
        safe_parts = [_safe_name_component(p) for p in parts]
        path = "/".join(safe_parts)
        return f"{self.SOURCE_TYPE}://{self.server_name}/{path}"
 
    def discover_assets(self) -> list[SourceAsset]:
        """Discover assets from the source system.
 
        In production, this would connect to the source and query metadata.
        Here we return sample data for demonstration.
        """
        logger.info(f"Discovering assets from {self.server_name}...")
 
        assets = []
 
        # Server
        assets.append(SourceAsset(
            name=self.server_name,
            qualified_name=self._qualified_name(),
            entity_type="custom_sql_server",
            attributes={"serverVersion": "SQL Server 2022", "environment": "production"},
        ))
 
        # Database
        db_name = "SalesDB"
        assets.append(SourceAsset(
            name=db_name,
            qualified_name=self._qualified_name(db_name),
            entity_type="custom_sql_database",
            attributes={"databaseEngine": "MSSQL"},
        ))
 
        # Source tables
        tables = [
            {"name": "Orders", "schema": "dbo", "rows": 1500000},
            {"name": "Customers", "schema": "dbo", "rows": 50000},
            {"name": "Products", "schema": "dbo", "rows": 2000},
        ]
        # LLM10: cap table discovery breadth (env-configurable; skips logged)
        tables = _apply_cap(tables, MAX_OBJECTS_PER_RUN, "SQL tables")
        for t in tables:
            # Validate SQL identifiers before they enter qualifiedNames/queries
            _validate_identifier(t["name"])
            _validate_identifier(t["schema"])
            assets.append(SourceAsset(
                name=t["name"],
                qualified_name=self._qualified_name(db_name, t["schema"], t["name"]),
                entity_type="custom_sql_table",
                attributes={"rowCount": t["rows"], "schemaName": t["schema"]},
            ))
 
        # Destination (aggregated) table
        assets.append(SourceAsset(
            name="OrderSummary",
            qualified_name=self._qualified_name(db_name, "analytics", "OrderSummary"),
            entity_type="custom_sql_table",
            attributes={"rowCount": 365000, "schemaName": "analytics"},
        ))
 
        # Columns for Orders table (demonstrating column-level metadata)
        # Classifications are driven by classification_rules.json via the
        # shared ClassificationEngine — no hardcoded classification logic.
        classification_engine = ClassificationEngine()
        logger.info(
            f"Classification engine loaded: "
            f"{classification_engine.get_stats()['total']} rules"
        )
        columns = [
            {"name": "OrderID", "type": "int", "nullable": False, "pk": True},
            {"name": "CustomerID", "type": "int", "nullable": False, "pk": False},
            {"name": "OrderDate", "type": "datetime", "nullable": False, "pk": False},
            {"name": "TotalAmount", "type": "decimal(18,2)", "nullable": True, "pk": False},
        ]
 
        # LLM10: cap per-table column breadth; validate each column identifier
        columns = _apply_cap(columns, MAX_FIELDS_PER_OBJECT, "columns on Orders")
        for col in columns:
            _validate_identifier(col["name"])

        # Classify all columns in the table using the shared engine.
        # Use the canonical name_key/type_key kwargs (supported by BOTH engine
        # versions) so this connector is not coupled to the field_name_key alias.
        col_classifications = classification_engine.classify_fields(
            source="sql",
            object_name="Orders",
            fields=columns,
            name_key="name",
            type_key="type",
        )
 
        for col in columns:
            col_class = col_classifications.get(col["name"])
            assets.append(SourceAsset(
                name=col["name"],
                qualified_name=self._qualified_name(db_name, "dbo", "Orders", col["name"]),
                entity_type="custom_sql_column",
                attributes={
                    "dataType": col["type"],
                    "isNullable": col["nullable"],
                    "isPrimaryKey": col["pk"],
                },
                classifications=[col_class] if col_class else [],
            ))
 
        logger.info(f"Discovered {len(assets)} assets")
        return assets
 
    def discover_lineage(self) -> list[LineageRelationship]:
        """Discover lineage relationships from the source system.
 
        In production, this might parse ETL job configs, stored procedures,
        or query logs to determine data flow.
        """
        logger.info("Discovering lineage relationships...")
 
        db_name = "SalesDB"
        relationships = [
            LineageRelationship(
                process_name="Daily Order Aggregation",
                process_qualified_name=self._qualified_name(db_name, "processes", "daily_order_agg"),
                process_type="StoredProcedure",
                input_qualified_names=[
                    self._qualified_name(db_name, "dbo", "Orders"),
                    self._qualified_name(db_name, "dbo", "Customers"),
                    self._qualified_name(db_name, "dbo", "Products"),
                ],
                output_qualified_names=[
                    self._qualified_name(db_name, "analytics", "OrderSummary"),
                ],
                query_text="EXEC analytics.sp_DailyOrderAggregation",
            ),
        ]
 
        logger.info(f"Discovered {len(relationships)} lineage relationships")
        return relationships
 
 
# =============================================================================
# 8. MAIN ORCHESTRATOR
# =============================================================================
 
def main():
    """Main entry point — orchestrates the full connector workflow."""
 
    logger.info("=" * 70)
    logger.info("Microsoft Purview Custom Connector - Example Run")
    logger.info("=" * 70)
 
    # --- Configuration ---
    config = PurviewConfig.from_environment()
    if not config.account_name:
        config.account_name = "my-purview-account"  # Default for dry run
        logger.warning("No PURVIEW_ACCOUNT_NAME set — running in DRY RUN mode")
 
    # --- Step 1: Authenticate ---
    logger.info("\n--- Step 1: Authentication ---")
    auth_service = AuthService(config)
    client = auth_service.get_purview_client()
 
    # --- Step 2: Register custom type definitions ---
    logger.info("\n--- Step 2: Register Type Definitions ---")
    typedef_service = TypeDefService(client, config, auth_service)
    typedef_service.register_types()
 
    # --- Step 3: Discover and create entities ---
    logger.info("\n--- Step 3: Discover and Create Entities ---")
    connector = SQLServerConnector(server_name="sql-prod-01.company.com", config=config)
    assets = connector.discover_assets()
 
    # LLM04: warn if asset descriptions drifted since the last run
    _check_metadata_drift(
        (a.qualified_name, str(a.attributes.get("description", "")))
        for a in assets
    )
 
    entity_service = EntityService(client, config, auth_service)
    entity_service.create_entities_bulk(assets)
 
    # --- Step 4: Create lineage ---
    logger.info("\n--- Step 4: Create Lineage ---")
    lineage_service = LineageService(client, config, auth_service)
    lineage_relationships = connector.discover_lineage()
    for rel in lineage_relationships:
        lineage_service.create_lineage(rel)
 
    # --- Step 5: Apply business metadata ---
    logger.info("\n--- Step 5: Apply Business Metadata ---")
    metadata_service = MetadataService(config, auth_service)
 
    # In real usage, you'd use the GUID returned from entity creation
    sample_guid = "00000000-0000-0000-0000-000000000001"
    if APPLY_BUSINESS_METADATA:
        # ⚠️ M3 PLACEHOLDER VALUES: replace with values computed from real
        # sources (data-quality jobs, ownership registries) before enabling
        # the flag. Fabricated governance signals mislead both humans and AI
        # features that treat the catalog as authoritative.
        metadata_service.apply_business_metadata(
            entity_guid=sample_guid,
            metadata={
                "DataQuality": {
                    "lastValidated": "REPLACE-ME",  # e.g., from your DQ pipeline
                    "qualityScore": None,            # e.g., computed score
                    "dataOwner": "REPLACE-ME",       # e.g., from ownership registry
                }
            },
        )
    else:
        logger.info(
            "Skipping business metadata (APPLY_BUSINESS_METADATA is not 'true'). "
            "Replace the placeholder values in Step 5 with real computed values "
            "before enabling."
        )
 
    metadata_service.apply_classifications(
        entity_guid=sample_guid,
        classification_names=["Confidential"],
    )
 
    # --- Summary ---
    logger.info("\n" + "=" * 70)
    logger.info("Connector run complete!")
    logger.info(f"  Entities created/updated: {len(assets)}")
    logger.info(f"  Lineage relationships:    {len(lineage_relationships)}")
    logger.info("=" * 70)
 
 
if __name__ == "__main__":
    main()
 
