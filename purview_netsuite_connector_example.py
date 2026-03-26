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
from dataclasses import dataclass
from typing import Optional

# Classification engine — loads rules from classification_rules.json
from classification_engine import ClassificationEngine

# --- Uncomment these imports when running against real Purview + NetSuite ---
# from pyapacheatlas.auth import ServicePrincipalAuthentication
# from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess
# from azure.identity import DefaultAzureCredential
# from azure.keyvault.secrets import SecretClient
# from requests_oauthlib import OAuth1
# from dotenv import load_dotenv
# import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Security: default timeout for all HTTP requests (seconds) ---
REQUEST_TIMEOUT = (10, 30)


def _validate_identifier(value: str, allow_list: list = None) -> str:
    """Validate that a string is a safe SuiteQL/API identifier."""
    import re
    if allow_list and value not in allow_list:
        raise ValueError(f"Identifier '{value}' is not in the allow-list.")
    if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', value):
        raise ValueError(f"Identifier '{value}' contains invalid characters.")
    return value


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
            response = requests.get(url, auth=auth, timeout=REQUEST_TIMEOUT)
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
        # response = requests.get(url, auth=self.auth.get_auth(), headers=self.auth.get_headers(),
        #                         timeout=REQUEST_TIMEOUT)
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

        logger.info(f"[DRY RUN] Would call GET {self.config.record_api_url}/metadata-catalog/{record_type}")
        return self._get_simulated_fields(record_type)

    def get_record_count(self, record_type: str) -> int:
        """Get the record count via SuiteQL query.

        Uses: POST /services/rest/query/v1/suiteql
        Body: {"q": "SELECT COUNT(*) AS cnt FROM {table}"}
        """
        # --- Uncomment for real usage ---
        # # Security: use an explicit allow-list map to prevent SuiteQL injection.
        # # Only pre-defined record types are allowed; arbitrary input is rejected.
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
        # if record_type not in table_map:
        #     raise ValueError(f"Record type '{record_type}' is not in the SuiteQL allow-list.")
        # table = table_map[record_type]
        # url = self.config.suiteql_url
        # payload = {"q": f"SELECT COUNT(*) AS cnt FROM {table}"}
        # response = requests.post(url, json=payload, auth=self.auth.get_auth(),
        #                          headers={**self.auth.get_headers(), "Prefer": "transient"},
        #                          timeout=REQUEST_TIMEOUT)
        # response.raise_for_status()
        # items = response.json().get("items", [])
        # return items[0].get("cnt", 0) if items else 0

        counts = {
            "customer": 4200, "vendor": 850, "employee": 320,
            "salesOrder": 18500, "invoice": 22300, "purchaseOrder": 6100,
            "vendorBill": 9400, "inventoryItem": 3750,
            "journalEntry": 15200, "account": 285,
        }
        return counts.get(record_type, 0)

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

    @staticmethod
    def register_types(purview_endpoint: str, bearer_token: str) -> dict:
        url = f"{purview_endpoint}/api/atlas/v2/types/typedefs"
        type_names = [t["name"] for t in TypeDefService.NETSUITE_TYPES["entityDefs"]]
        logger.info(f"[DRY RUN] Would POST to {url}")
        logger.info(f"[DRY RUN] Would register {len(type_names)} types: {type_names}")
        return TypeDefService.NETSUITE_TYPES


# =============================================================================
# 5-7. ENTITY, LINEAGE, AND METADATA SERVICES
# =============================================================================

class EntityService:
    BATCH_SIZE = 50

    @staticmethod
    def build_entity(type_name, qualified_name, name, description="", attributes=None):
        return {"typeName": type_name, "attributes": {
            "qualifiedName": qualified_name, "name": name,
            "description": description, **(attributes or {}),
        }, "status": "ACTIVE"}

    @staticmethod
    def create_entities_bulk(purview_endpoint, bearer_token, entities):
        url = f"{purview_endpoint}/api/atlas/v2/entity/bulk"
        for i in range(0, len(entities), EntityService.BATCH_SIZE):
            batch = entities[i : i + EntityService.BATCH_SIZE]
            logger.info(f"[DRY RUN] Would POST batch of {len(batch)} entities to {url}")
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
        logger.info(f"[DRY RUN] Would apply business metadata to entity {entity_guid}: {metadata}")

    @staticmethod
    def apply_classification(purview_endpoint, bearer_token, entity_guid, classification_name):
        logger.info(f"[DRY RUN] Would apply classification '{classification_name}' to entity {entity_guid}")


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
            rt = rec["record_type"]
            fields = self.discovery.discover_record_fields(rt)
            record_details[rt] = fields
            count = self.discovery.get_record_count(rt)

            rec_qn = f"netsuite://{self.account_name}/{rt}"
            all_entities.append(EntityService.build_entity(
                "custom_netsuite_record_type", rec_qn, rec["display_name"],
                rec["description"],
                {"recordTypeName": rt, "recordCategory": rec["category"], "recordCount": count},
            ))

            for fld in fields:
                fld_qn = f"netsuite://{self.account_name}/{rt}/{fld['name']}"
                all_entities.append(EntityService.build_entity(
                    "custom_netsuite_field", fld_qn,
                    fld.get("title", fld["name"]),
                    f"Field {fld['name']} on {rt} (type: {fld['type']})",
                    {"fieldType": fld["type"], "isRequired": fld.get("required", False),
                     "isReadOnly": fld.get("readOnly", False),
                     "isReference": fld["type"] == "object"},
                ))

        # Step 4: Push entities
        logger.info(f"\n--- Step 4: Create {len(all_entities)} Entities in Purview ---")
        EntityService.create_entities_bulk(purview_endpoint, purview_token, all_entities)

        # Step 5: Build lineage
        logger.info("\n--- Step 5: Build Cross-System Lineage ---")
        process_entities = []
        for mapping in lineage_mappings:
            source_qns = [f"netsuite://{self.account_name}/{r}" for r in mapping["source_records"]]
            source_types = ["custom_netsuite_record_type"] * len(mapping["source_records"])
            process_qn = f"netsuite://{self.account_name}/process/{mapping['process_name'].replace(' ', '_').lower()}"

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
        MetadataService.apply_business_metadata(
            purview_endpoint, purview_token, "dry-run-guid-customer-001",
            {"DataQuality": {"lastValidated": "2026-02-20T03:00:00Z", "qualityScore": 91.8,
                             "dataOwner": "Finance Operations", "dataSteward": "ERP Admin"}},
        )

        # Classify fields — now driven by classification_rules.json
        classification_engine = ClassificationEngine()
        logger.info(f"  Classification engine loaded: {classification_engine.get_stats()['total']} rules")

        total_classified = 0
        for rec in record_types:
            rec_name = rec["record_type"]
            rec_fields = record_details.get(rec_name, [])
            if not rec_fields:
                continue

            classifications = classification_engine.classify_fields(
                source="netsuite",
                object_name=rec_name,
                fields=rec_fields,
                field_name_key="name",
                field_type_key="type",
            )

            for field_name, classification_type in classifications.items():
                MetadataService.apply_classification(
                    purview_endpoint, purview_token,
                    f"dry-run-guid-{rec_name}-{field_name}", classification_type,
                )
                total_classified += 1

        logger.info(f"  Total fields classified across all record types: {total_classified}")

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
        logger.info(f"  Sensitive fields classified: {total_classified}")


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
