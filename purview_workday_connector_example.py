"""
Microsoft Purview Custom Connector for Workday - Example Implementation
=========================================================================

This example demonstrates a complete custom connector that:
1. Authenticates to Workday via OAuth 2.0 (REST API) with refresh tokens
2. Authenticates to Purview using Managed Identity / Service Principal
3. Discovers Workday business objects and fields via the REST API
4. Registers custom type definitions in Purview for Workday assets
5. Creates entities (tenant → business objects → fields hierarchy) via Atlas v2
6. Builds cross-system lineage (Workday → ETL → Data Warehouse / AD / downstream)
7. Applies business metadata and classifications (including PII tagging)

This is the SAME approach used for Salesforce and SQL Server — the only difference
is which source API we call to discover metadata. The Purview side is identical.


PREREQUISITES
-------------
1. Python 3.9+ installed on your development machine or deployed to Azure Functions.
   Verify with: python --version

2. Install the required Python packages:

       pip install pyapacheatlas azure-identity azure-keyvault-secrets requests python-dotenv

   If deploying to Azure Functions, add these to your requirements.txt:

       azure-functions>=1.17.0
       pyapacheatlas>=0.14.0
       azure-identity>=1.15.0
       azure-keyvault-secrets>=4.8.0
       requests>=2.31.0
       python-dotenv>=1.0.0

3. A Microsoft Purview account (Data Map enabled) in your Azure subscription.
   https://learn.microsoft.com/en-us/purview/create-microsoft-purview-portal

4. An Azure Key Vault to securely store Workday credentials.
   Azure portal > Create a resource > Key Vault > Create.

5. A Workday tenant (Production, Sandbox, or Implementation) with API access enabled.

6. A Workday API Client and Integration System User (ISU) configured for API access.
   See Architecture Document Section 3.3 for full step-by-step detail. Summary:

   a. In Workday, search for "Register API Client for Integrations".
   b. Enter a Client Name (e.g., "Purview Metadata Connector").
   c. Choose Functional Area scopes (Human Resources, Staffing, Organizations, System).
   d. Optionally check "Non-Expiring Refresh Tokens".
   e. Note the Client ID; copy the Client Secret immediately (shown once).
   f. Create an Integration System User (ISU):
      - Search for "Create Integration System User" in Workday.
      - Set username (e.g., ISU_Purview_Connector) and password.
      - Set Session Timeout Minutes to 0.
   g. Create an Integration System Security Group (ISSG) and add the ISU.
   h. Grant the ISSG GET permissions on required domains (Worker Data,
      Organization Data, Job Data, Compensation Data, etc.).
   i. Activate pending security policy changes.
   j. Generate a refresh token under "Manage Refresh Tokens for Integrations".

7. Authentication to Purview — choose one of:

   a. Managed Identity (recommended for Azure-hosted deployments):
      - Enable System-Assigned Managed Identity on your Azure Function App.
      - Assign "Data Curator" and "Data Source Administrator" roles in Purview
        governance portal (Data Map > Collections > Role assignments).
      - Grant "Key Vault Secrets User" role on your Azure Key Vault.
      - No AZURE_TENANT_ID/CLIENT_ID/CLIENT_SECRET environment variables needed.

   b. Service Principal (for local development or non-Azure environments):
      - Register an App in Microsoft Entra ID.
      - Create a client secret.
      - Assign Purview roles as above.
      - Set AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET env vars.

8. Where to run this connector:

   LOCAL DEVELOPMENT:  Run directly with Python + .env file + Service Principal.
   AZURE FUNCTIONS:    Deploy as Timer/HTTP Trigger with Managed Identity (recommended).
   CONTAINER APPS:     For jobs exceeding Azure Functions timeout limits.
   ON-PREMISES:        Scheduled task with Service Principal + network access to both systems.


ENVIRONMENT VARIABLES
---------------------
Required for ALL environments:
    PURVIEW_ACCOUNT_NAME   Purview account name (without .purview.azure.com).
                           Where to find: Azure portal > Purview resource > Overview.

    KEY_VAULT_URL          Full URL of your Azure Key Vault.
                           Example: "https://my-keyvault.vault.azure.net/"

Required ONLY for Service Principal auth (local dev / non-Azure):
    AZURE_TENANT_ID        Microsoft Entra ID tenant ID.
    AZURE_CLIENT_ID        Application (client) ID of your registered app.
    AZURE_CLIENT_SECRET    Client secret value (only shown once at creation).

NOT required with Managed Identity:
    DefaultAzureCredential auto-detects Managed Identity in Azure.

Example .env file:
    # .env — DO NOT COMMIT TO SOURCE CONTROL
    AZURE_TENANT_ID=12a345bc-67d1-ef89-abcd-efg12345abcde
    AZURE_CLIENT_ID=a1234bcd-5678-9012-abcd-abcd1234abcd
    AZURE_CLIENT_SECRET=xYz...your-secret-value...
    PURVIEW_ACCOUNT_NAME=my-purview-account
    KEY_VAULT_URL=https://my-keyvault.vault.azure.net/


AZURE KEY VAULT SECRETS REQUIRED
---------------------------------
Create these in: Azure portal > Key Vault > Objects > Secrets > + Generate/Import

    Secret Name                Description                                      Example Value
    -------------------------  -----------------------------------------------  ---------------------------
    workday-client-id          Client ID from Register API Client.              MWYxZDc3ZjEtYmVi...
                               Found under View API Clients in Workday.

    workday-client-secret      Client Secret generated at registration.         abc123secret...
                               Only displayed once — copy immediately.

    workday-refresh-token      Refresh token generated for the ISU.             def456token...
                               Found under Manage Refresh Tokens for
                               Integrations in Workday.

    workday-tenant-url         Base URL for your Workday tenant.                https://wd5-impl-services1.workday.com
                               The host varies by data center (wd2, wd3, wd5).
                               Found under View API Clients > REST API Endpoint.

    workday-tenant-name        Your Workday tenant name (in API paths).         mycompany
                               Found in the REST API endpoint URL:
                               https://host/ccx/api/v1/{tenant_name}/...

Grant read access: Key Vault > Access control (IAM) > Add role assignment >
    Role: "Key Vault Secrets User" > Members: your Function App or App Registration.


USAGE
-----
1. DRY-RUN MODE (default, no credentials needed):
       python purview_workday_connector_example.py

2. LIVE MODE: Uncomment all "--- Uncomment for real usage ---" blocks, set env vars.

3. AZURE FUNCTIONS:
       import azure.functions as func
       from purview_workday_connector_example import main as run_connector

       app = func.FunctionApp()

       @app.timer_trigger(schedule="0 0 2 * * *", arg_name="timer")
       def purview_workday_scan(timer: func.TimerRequest) -> None:
           run_connector()

4. CUSTOMIZATION: Edit OBJECTS_TO_SCAN and LINEAGE_MAPPINGS below.
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

# Classification engine — loads rules from classification_rules.json
from classification_engine import ClassificationEngine

# --- Uncomment these imports when running against real Purview + Workday ---
# from pyapacheatlas.auth import ServicePrincipalAuthentication
# from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess
# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
# from dotenv import load_dotenv
# import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Security: default timeout for all HTTP requests (seconds) ---
REQUEST_TIMEOUT = (10, 30)


def _validate_identifier(value: str, allow_list: list = None) -> str:
    """Validate that a string is a safe API identifier."""
    import re
    if allow_list and value not in allow_list:
        raise ValueError(f"Identifier '{value}' is not in the allow-list.")
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', value):
        raise ValueError(f"Identifier '{value}' contains invalid characters.")
    return value


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

WORKDAY_API_VERSION = "v1"

OBJECTS_TO_SCAN = [
    "workers", "organizations", "supervisoryOrganizations", "positions",
    "jobProfiles", "compensationPlans", "costCenters", "locations",
    "companies", "payGroups",
]

OBJECT_METADATA = {
    "workers": {
        "label": "Workers", "description": "Employee and contingent worker records",
        "soap_operation": "Get_Workers", "functional_area": "HCM",
        "fields": [
            {"name": "workerId", "label": "Worker ID", "type": "string", "isPII": True},
            {"name": "descriptor", "label": "Full Name", "type": "string", "isPII": True},
            {"name": "businessTitle", "label": "Business Title", "type": "string", "isPII": False},
            {"name": "primaryWorkEmail", "label": "Work Email", "type": "email", "isPII": True},
            {"name": "primaryWorkPhone", "label": "Work Phone", "type": "phone", "isPII": True},
            {"name": "hireDate", "label": "Hire Date", "type": "date", "isPII": False},
            {"name": "terminationDate", "label": "Termination Date", "type": "date", "isPII": False},
            {"name": "workerType", "label": "Worker Type", "type": "string", "isPII": False},
            {"name": "managementLevel", "label": "Management Level", "type": "reference", "isPII": False},
            {"name": "supervisoryOrganization", "label": "Supervisory Org", "type": "reference", "isPII": False},
            {"name": "location", "label": "Location", "type": "reference", "isPII": False},
            {"name": "costCenter", "label": "Cost Center", "type": "reference", "isPII": False},
        ],
    },
    "organizations": {
        "label": "Organizations", "description": "Organizational hierarchy and structure",
        "soap_operation": "Get_Organizations", "functional_area": "HCM",
        "fields": [
            {"name": "organizationId", "label": "Organization ID", "type": "string", "isPII": False},
            {"name": "organizationName", "label": "Organization Name", "type": "string", "isPII": False},
            {"name": "organizationType", "label": "Organization Type", "type": "string", "isPII": False},
            {"name": "organizationSubType", "label": "Sub Type", "type": "string", "isPII": False},
            {"name": "isActive", "label": "Is Active", "type": "boolean", "isPII": False},
            {"name": "superiorOrganization", "label": "Parent Organization", "type": "reference", "isPII": False},
        ],
    },
    "supervisoryOrganizations": {
        "label": "Supervisory Organizations", "description": "Manager-based reporting structure",
        "soap_operation": "Get_Supervisory_Organizations", "functional_area": "HCM",
        "fields": [
            {"name": "orgId", "label": "Org ID", "type": "string", "isPII": False},
            {"name": "orgName", "label": "Org Name", "type": "string", "isPII": False},
            {"name": "manager", "label": "Manager", "type": "reference", "isPII": True},
            {"name": "headcount", "label": "Headcount", "type": "integer", "isPII": False},
            {"name": "superiorOrg", "label": "Superior Org", "type": "reference", "isPII": False},
        ],
    },
    "positions": {
        "label": "Positions", "description": "Position definitions and assignments",
        "soap_operation": "Get_Positions", "functional_area": "HCM",
        "fields": [
            {"name": "positionId", "label": "Position ID", "type": "string", "isPII": False},
            {"name": "positionTitle", "label": "Position Title", "type": "string", "isPII": False},
            {"name": "jobProfile", "label": "Job Profile", "type": "reference", "isPII": False},
            {"name": "workerAssigned", "label": "Worker Assigned", "type": "reference", "isPII": True},
            {"name": "isFilled", "label": "Is Filled", "type": "boolean", "isPII": False},
            {"name": "availableDate", "label": "Available Date", "type": "date", "isPII": False},
        ],
    },
    "jobProfiles": {
        "label": "Job Profiles", "description": "Job profile definitions with pay grades and job families",
        "soap_operation": "Get_Job_Profiles", "functional_area": "HCM",
        "fields": [
            {"name": "jobProfileId", "label": "Job Profile ID", "type": "string", "isPII": False},
            {"name": "jobProfileName", "label": "Job Profile Name", "type": "string", "isPII": False},
            {"name": "jobFamily", "label": "Job Family", "type": "reference", "isPII": False},
            {"name": "jobLevel", "label": "Job Level", "type": "string", "isPII": False},
            {"name": "payRateType", "label": "Pay Rate Type", "type": "string", "isPII": False},
            {"name": "isActive", "label": "Is Active", "type": "boolean", "isPII": False},
        ],
    },
    "compensationPlans": {
        "label": "Compensation Plans", "description": "Salary and compensation plan structures",
        "soap_operation": "Get_Compensation_Plans", "functional_area": "Compensation",
        "fields": [
            {"name": "planId", "label": "Plan ID", "type": "string", "isPII": False},
            {"name": "planName", "label": "Plan Name", "type": "string", "isPII": False},
            {"name": "compensationType", "label": "Compensation Type", "type": "string", "isPII": False},
            {"name": "currency", "label": "Currency", "type": "string", "isPII": False},
            {"name": "effectiveDate", "label": "Effective Date", "type": "date", "isPII": False},
        ],
    },
    "costCenters": {
        "label": "Cost Centers", "description": "Financial cost center hierarchy",
        "soap_operation": "Get_Cost_Centers", "functional_area": "Finance",
        "fields": [
            {"name": "costCenterId", "label": "Cost Center ID", "type": "string", "isPII": False},
            {"name": "costCenterName", "label": "Cost Center Name", "type": "string", "isPII": False},
            {"name": "costCenterCode", "label": "Code", "type": "string", "isPII": False},
            {"name": "isActive", "label": "Is Active", "type": "boolean", "isPII": False},
        ],
    },
    "locations": {
        "label": "Locations", "description": "Physical location definitions",
        "soap_operation": "Get_Locations", "functional_area": "HCM",
        "fields": [
            {"name": "locationId", "label": "Location ID", "type": "string", "isPII": False},
            {"name": "locationName", "label": "Location Name", "type": "string", "isPII": False},
            {"name": "locationType", "label": "Location Type", "type": "string", "isPII": False},
            {"name": "country", "label": "Country", "type": "string", "isPII": False},
            {"name": "addressLine1", "label": "Address", "type": "string", "isPII": False},
            {"name": "isActive", "label": "Is Active", "type": "boolean", "isPII": False},
        ],
    },
    "companies": {
        "label": "Companies", "description": "Legal entity / company definitions",
        "soap_operation": "Get_Organizations", "functional_area": "Finance",
        "fields": [
            {"name": "companyId", "label": "Company ID", "type": "string", "isPII": False},
            {"name": "companyName", "label": "Company Name", "type": "string", "isPII": False},
            {"name": "country", "label": "Country", "type": "string", "isPII": False},
            {"name": "currency", "label": "Currency", "type": "string", "isPII": False},
        ],
    },
    "payGroups": {
        "label": "Pay Groups", "description": "Payroll grouping definitions",
        "soap_operation": "Get_Pay_Groups", "functional_area": "Payroll",
        "fields": [
            {"name": "payGroupId", "label": "Pay Group ID", "type": "string", "isPII": False},
            {"name": "payGroupName", "label": "Pay Group Name", "type": "string", "isPII": False},
            {"name": "payFrequency", "label": "Pay Frequency", "type": "string", "isPII": False},
            {"name": "country", "label": "Country", "type": "string", "isPII": False},
        ],
    },
}

LINEAGE_MAPPINGS = [
    {"source_objects": ["workers", "positions", "supervisoryOrganizations"],
     "process_name": "HR Data Warehouse Sync", "process_type": "ETL",
     "destination_table": "dwh://analytics-warehouse/hr/dim_employee", "destination_type": "custom_sql_table",
     "description": "Daily sync of worker, position, and org data to the employee dimension table"},
    {"source_objects": ["compensationPlans", "workers"],
     "process_name": "Compensation Analytics Sync", "process_type": "ETL",
     "destination_table": "dwh://analytics-warehouse/hr/fact_compensation", "destination_type": "custom_sql_table",
     "description": "Daily sync of compensation data to the compensation fact table"},
    {"source_objects": ["organizations", "costCenters", "companies"],
     "process_name": "Org Structure Sync", "process_type": "ETL",
     "destination_table": "dwh://analytics-warehouse/hr/dim_organization", "destination_type": "custom_sql_table",
     "description": "Daily sync of organizational hierarchy to the organization dimension table"},
    {"source_objects": ["workers"],
     "process_name": "Active Directory Provisioning", "process_type": "API Sync",
     "destination_table": "ad://corp.mycompany.com/users", "destination_type": "DataSet",
     "description": "Real-time provisioning of new hires and terminations to Active Directory"},
]


@dataclass
class PurviewConfig:
    tenant_id: str = ""; client_id: str = ""; client_secret: str = ""; account_name: str = ""
    @classmethod
    def from_environment(cls):
        return cls(tenant_id=os.environ.get("AZURE_TENANT_ID",""), client_id=os.environ.get("AZURE_CLIENT_ID",""),
                   client_secret=os.environ.get("AZURE_CLIENT_SECRET",""), account_name=os.environ.get("PURVIEW_ACCOUNT_NAME",""))
    @property
    def endpoint(self): return f"https://{self.account_name}.purview.azure.com/datamap"

@dataclass
class WorkdayConfig:
    client_id: str = ""; client_secret: str = ""; refresh_token: str = ""
    tenant_url: str = ""; tenant_name: str = ""; access_token: str = ""
    api_version: str = WORKDAY_API_VERSION
    @classmethod
    def from_key_vault(cls, kv_url):
        # --- Uncomment for real usage ---
        # credential = DefaultAzureCredential()
        # kv_client = SecretClient(vault_url=kv_url, credential=credential)
        # return cls(client_id=kv_client.get_secret("workday-client-id").value,
        #            client_secret=kv_client.get_secret("workday-client-secret").value,
        #            refresh_token=kv_client.get_secret("workday-refresh-token").value,
        #            tenant_url=kv_client.get_secret("workday-tenant-url").value,
        #            tenant_name=kv_client.get_secret("workday-tenant-name").value)
        logger.info(f"[DRY RUN] Would retrieve Workday credentials from Key Vault: {kv_url}")
        return cls(client_id="dry-run-id", client_secret="dry-run-secret", refresh_token="dry-run-token",
                   tenant_url="https://wd5-impl-services1.workday.com", tenant_name="mycompany")
    @property
    def base_api_url(self): return f"{self.tenant_url}/ccx/api/{self.api_version}/{self.tenant_name}"
    @property
    def token_url(self): return f"{self.tenant_url}/ccx/oauth2/{self.tenant_name}/token"


# =============================================================================
# 2. AUTHENTICATION SERVICES
# =============================================================================

class PurviewAuthService:
    def __init__(self, config): self.config = config
    def get_bearer_token(self):
        logger.info("[DRY RUN] Would acquire Purview bearer token via DefaultAzureCredential")
        return "dry-run-purview-token"

class WorkdayAuthService:
    """OAuth 2.0 with refresh tokens. POST {tenant_url}/ccx/oauth2/{tenant}/token
    Body: grant_type=refresh_token&client_id=...&client_secret=...&refresh_token=..."""
    def __init__(self, config): self.config = config
    def authenticate(self):
        # --- Uncomment for real usage ---
        # response = requests.post(self.config.token_url, data={
        #     "grant_type": "refresh_token", "client_id": self.config.client_id,
        #     "client_secret": self.config.client_secret, "refresh_token": self.config.refresh_token},
        #     timeout=REQUEST_TIMEOUT)
        # response.raise_for_status()
        # self.config.access_token = response.json()["access_token"]
        logger.info(f"[DRY RUN] Would authenticate to Workday at: {self.config.token_url}")
        self.config.access_token = "dry-run-wd-token"
        return self.config
    def get_headers(self):
        return {"Authorization": f"Bearer {self.config.access_token}", "Content-Type": "application/json"}


# =============================================================================
# 3. WORKDAY METADATA DISCOVERY SERVICE
# =============================================================================

class WorkdayDiscoveryService:
    """Discovers metadata from Workday REST API.
    Key endpoints: GET /ccx/api/v1/{tenant}/workers, /organizations, /positions, etc.
    Each returns JSON with a 'data' array. Field-level metadata is defined in OBJECT_METADATA
    since Workday does not have a direct 'describe' endpoint like Salesforce."""
    def __init__(self, config, auth): self.config = config; self.auth = auth

    def discover_objects(self, object_filter=None):
        objects = []
        for name in (object_filter or OBJECTS_TO_SCAN):
            meta = OBJECT_METADATA.get(name, {})
            objects.append({"name": name, "label": meta.get("label", name),
                           "description": meta.get("description", ""),
                           "soap_operation": meta.get("soap_operation", ""),
                           "functional_area": meta.get("functional_area", "HCM")})
        logger.info(f"[DRY RUN] Discovered {len(objects)} Workday business objects")
        return objects

    def get_object_fields(self, object_name):
        return OBJECT_METADATA.get(object_name, {}).get("fields", [])

    def get_record_count(self, object_name):
        # GET {base_url}/{object_name}?limit=1 → response["total"]
        counts = {"workers": 8500, "organizations": 120, "supervisoryOrganizations": 340,
                  "positions": 9200, "jobProfiles": 275, "compensationPlans": 45,
                  "costCenters": 85, "locations": 52, "companies": 8, "payGroups": 15}
        return counts.get(object_name, 0)


# =============================================================================
# 4-7. PURVIEW SERVICES (Type Defs, Entities, Lineage, Metadata)
# =============================================================================

class TypeDefService:
    WORKDAY_TYPES = {"entityDefs": [
        {"category": "ENTITY", "name": "custom_workday_tenant", "description": "A Workday tenant",
         "superTypes": ["Server"], "typeVersion": "1.0", "attributeDefs": [
             {"name": "tenantName", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
             {"name": "tenantUrl", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
             {"name": "apiVersion", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
             {"name": "environment", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True}]},
        {"category": "ENTITY", "name": "custom_workday_object", "description": "A Workday business object",
         "superTypes": ["DataSet"], "typeVersion": "1.0", "attributeDefs": [
             {"name": "apiEndpoint", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
             {"name": "soapOperation", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
             {"name": "recordCount", "typeName": "long", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
             {"name": "functionalArea", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True}]},
        {"category": "ENTITY", "name": "custom_workday_field", "description": "A field on a Workday business object",
         "superTypes": ["DataSet"], "typeVersion": "1.0", "attributeDefs": [
             {"name": "fieldType", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
             {"name": "isPII", "typeName": "boolean", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
             {"name": "referenceTo", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True}]},
        {"category": "ENTITY", "name": "custom_workday_process", "description": "A data movement process from Workday",
         "superTypes": ["Process"], "typeVersion": "1.0", "attributeDefs": [
             {"name": "processType", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": True},
             {"name": "schedule", "typeName": "string", "isOptional": True, "cardinality": "SINGLE", "isUnique": False, "isIndexable": False}]},
    ]}
    @staticmethod
    def register_types(endpoint, token):
        names = [t["name"] for t in TypeDefService.WORKDAY_TYPES["entityDefs"]]
        logger.info(f"[DRY RUN] Would register {len(names)} types: {names}")
        return TypeDefService.WORKDAY_TYPES

class EntityService:
    BATCH_SIZE = 50
    @staticmethod
    def build_entity(type_name, qualified_name, name, description="", attributes=None):
        return {"typeName": type_name, "attributes": {"qualifiedName": qualified_name, "name": name,
                "description": description, **(attributes or {})}, "status": "ACTIVE"}
    @staticmethod
    def create_entities_bulk(endpoint, token, entities):
        for i in range(0, len(entities), EntityService.BATCH_SIZE):
            batch = entities[i:i+EntityService.BATCH_SIZE]
            logger.info(f"[DRY RUN] Would POST batch of {len(batch)} entities")
            for e in batch: logger.info(f"  -> {e['attributes']['qualifiedName']}")

class LineageService:
    @staticmethod
    def build_process_entity(qn, name, ptype, in_qns, in_types, out_qns, out_types, desc="", sched=""):
        inputs = [{"typeName": t, "uniqueAttributes": {"qualifiedName": q}} for q, t in zip(in_qns, in_types)]
        outputs = [{"typeName": t, "uniqueAttributes": {"qualifiedName": q}} for q, t in zip(out_qns, out_types)]
        return {"typeName": "custom_workday_process", "attributes": {"qualifiedName": qn, "name": name,
                "description": desc, "processType": ptype, "schedule": sched,
                "inputs": inputs, "outputs": outputs}, "status": "ACTIVE"}

class MetadataService:
    @staticmethod
    def apply_business_metadata(endpoint, token, guid, metadata):
        logger.info(f"[DRY RUN] Would apply business metadata to {guid}: {metadata}")
    @staticmethod
    def apply_classification(endpoint, token, guid, classification):
        logger.info(f"[DRY RUN] Would apply classification '{classification}' to {guid}")


# =============================================================================
# 8. WORKDAY CONNECTOR (ORCHESTRATOR)
# =============================================================================

class WorkdayConnector:
    def __init__(self, purview_config, workday_config, tenant_label="mycompany"):
        self.pv = purview_config; self.wd = workday_config; self.label = tenant_label
        self.pv_auth = PurviewAuthService(purview_config)
        self.wd_auth = WorkdayAuthService(workday_config)
        self.discovery = WorkdayDiscoveryService(workday_config, self.wd_auth)

    def run(self, objects_to_scan=None, lineage_mappings=None):
        objects_to_scan = objects_to_scan or OBJECTS_TO_SCAN
        lineage_mappings = lineage_mappings or LINEAGE_MAPPINGS
        logger.info("=" * 70); logger.info("WORKDAY -> PURVIEW CUSTOM CONNECTOR"); logger.info("=" * 70)

        # Step 1: Authenticate
        logger.info("\n--- Step 1: Authentication ---")
        token = self.pv_auth.get_bearer_token(); self.wd_auth.authenticate()
        ep = self.pv.endpoint

        # Step 2: Register types
        logger.info("\n--- Step 2: Register Workday Custom Types ---")
        TypeDefService.register_types(ep, token)

        # Step 3: Discover metadata
        logger.info("\n--- Step 3: Discover Workday Metadata ---")
        objects = self.discovery.discover_objects(objects_to_scan)
        all_entities = []

        # Tenant entity
        all_entities.append(EntityService.build_entity("custom_workday_tenant",
            f"workday://{self.label}", f"Workday - {self.label}",
            f"Workday tenant: {self.label}",
            {"tenantName": self.wd.tenant_name, "tenantUrl": self.wd.tenant_url,
             "apiVersion": self.wd.api_version, "environment": "production"}))

        # Object + field entities
        for obj in objects:
            name = obj["name"]; count = self.discovery.get_record_count(name)
            all_entities.append(EntityService.build_entity("custom_workday_object",
                f"workday://{self.label}/{name}", obj["label"], obj["description"],
                {"apiEndpoint": f"/ccx/api/v1/{self.wd.tenant_name}/{name}",
                 "soapOperation": obj.get("soap_operation",""), "recordCount": count,
                 "functionalArea": obj.get("functional_area","HCM")}))
            for fld in self.discovery.get_object_fields(name):
                all_entities.append(EntityService.build_entity("custom_workday_field",
                    f"workday://{self.label}/{name}/{fld['name']}", fld["label"],
                    f"Field {fld['name']} on {name} (type: {fld['type']})",
                    {"fieldType": fld["type"], "isPII": fld.get("isPII", False), "referenceTo": ""}))

        # Step 4: Push entities
        logger.info(f"\n--- Step 4: Create {len(all_entities)} Entities in Purview ---")
        EntityService.create_entities_bulk(ep, token, all_entities)

        # Step 5: Lineage
        logger.info("\n--- Step 5: Build Cross-System Lineage ---")
        procs = []
        for m in lineage_mappings:
            src_qns = [f"workday://{self.label}/{o}" for o in m["source_objects"]]
            pqn = f"workday://{self.label}/process/{m['process_name'].replace(' ','_').lower()}"
            procs.append(LineageService.build_process_entity(pqn, m["process_name"], m["process_type"],
                src_qns, ["custom_workday_object"]*len(m["source_objects"]),
                [m["destination_table"]], [m["destination_type"]], m.get("description",""), "Daily 2:00 AM UTC"))
            logger.info(f"  Lineage: {m['source_objects']} -> {m['process_name']} -> {m['destination_table']}")
        EntityService.create_entities_bulk(ep, token, procs)

        # Step 6: Metadata & classifications
        logger.info("\n--- Step 6: Apply Business Metadata and Classifications ---")
        MetadataService.apply_business_metadata(ep, token, "dry-run-guid-workers",
            {"DataQuality": {"lastValidated": "2026-02-16T02:00:00Z", "qualityScore": 97.2,
                             "dataOwner": "HR Systems Team", "dataSteward": "John Doe"}})

        # Classify fields — now driven by classification_rules.json instead of isPII flags
        classification_engine = ClassificationEngine()
        logger.info(f"  Classification engine loaded: {classification_engine.get_stats()['total']} rules")

        pii_count = 0
        for obj_name in objects_to_scan:
            obj_fields = OBJECT_METADATA.get(obj_name, {}).get("fields", [])
            if not obj_fields:
                continue

            classifications = classification_engine.classify_fields(
                source="workday",
                object_name=obj_name,
                fields=obj_fields,
                field_name_key="name",
                field_type_key="type",
            )

            for field_name, classification_type in classifications.items():
                MetadataService.apply_classification(ep, token, f"dry-run-{field_name}", classification_type)
                pii_count += 1

        # Summary
        field_count = sum(len(OBJECT_METADATA.get(o["name"],{}).get("fields",[])) for o in objects)
        logger.info("\n" + "=" * 70); logger.info("CONNECTOR RUN COMPLETE"); logger.info("=" * 70)
        logger.info(f"  Tenant entity:    1")
        logger.info(f"  Object entities:  {len(objects)}")
        logger.info(f"  Field entities:   {field_count}")
        logger.info(f"  Process entities: {len(procs)}")
        logger.info(f"  Total entities:   {1 + len(objects) + field_count + len(procs)}")
        logger.info(f"  PII fields classified: {pii_count}")


# =============================================================================
# 9. MAIN ENTRY POINT
# =============================================================================

def main():
    logger.info("Initializing Workday -> Purview custom connector...")
    pv = PurviewConfig.from_environment()
    wd = WorkdayConfig.from_key_vault(os.environ.get("KEY_VAULT_URL", "https://my-keyvault.vault.azure.net/"))
    WorkdayConnector(pv, wd, "mycompany").run()

if __name__ == "__main__":
    main()
