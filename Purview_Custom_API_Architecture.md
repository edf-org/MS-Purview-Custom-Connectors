# Microsoft Purview Custom API Solution
 
## Architecture Document & Implementation Guide
 
**Version:** 1.3 **Date:** February 2026 **Last Reviewed:** July 2026 (verified against current Microsoft, Salesforce, Oracle NetSuite, and Workday documentation; v1.2 added the security controls and OWASP GenAI considerations from the Purview Connector Security Review v1.1; v1.3 adds Section 4.4.1 Configuration-Driven Classification Engine and Section 4.5 Scan Run Stamping & Rollback, unifying previously separate document branches) **Status:** Draft
 
## 1. Executive Summary
 
This document outlines the architecture for a custom API solution that integrates multiple data sources with Microsoft Purview. The solution enables automated metadata ingestion, custom lineage creation, and business metadata management using a modular Python-based framework.
 
The recommended approach combines the **pyapacheatlas** SDK for high-level operations with direct **Atlas v2 REST API** calls for advanced scenarios, deployed as **Azure Functions** for production scalability.
 
## 2. Solution Architecture Overview
 
### 2.1 High-Level Architecture
 
┌─────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION LAYER                       │
│              (Azure Functions / Timer Triggers)              │
└──────────┬──────────────┬──────────────┬────────────────────┘
           │              │              │
           ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  Source       │ │  Source       │ │  Source       │
│  Connector A  │ │  Connector B  │ │  Connector C  │
│  (e.g. SQL)   │ │  (e.g. SaaS)  │ │  (e.g. Files) │
└──────┬───────┘ └──────┬───────┘ └──────┬───────┘
       │                │                │
       ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────┐
│                   CORE SERVICE LAYER                         │
│                                                              │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐    │
│  │  Auth        │  │  Entity      │  │  Lineage          │    │
│  │  Manager     │  │  Manager     │  │  Builder           │    │
│  └─────────────┘  └─────────────┘  └──────────────────┘    │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐    │
│  │  TypeDef     │  │  Business    │  │  Classification    │    │
│  │  Manager     │  │  Metadata    │  │  Manager           │    │
│  └─────────────┘  └─────────────┘  └──────────────────┘    │
│                                                              │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  PURVIEW DATA MAP                            │
│            (Atlas v2 REST API / pyapacheatlas)               │
│                                                              │
│   Endpoint: https://{account}.purview.azure.com/datamap     │
└─────────────────────────────────────────────────────────────┘
 
### 2.2 Component Summary
 
| Component | Purpose | Technology |
| --- | --- | --- |
| Orchestration Layer | Scheduling, triggering, and coordinating connector runs | Azure Functions (Timer/HTTP triggers) |
| Source Connectors | Extract metadata from each data source | Python modules (one per source) |
| Core Service Layer | Shared services for auth, entity CRUD, lineage, types | pyapacheatlas + azure-identity |
| Purview Data Map | Target catalog for all metadata and lineage | Atlas v2 REST APIs |
 
## 3. Authentication Strategy
 
### 3.0 Which Authentication Should You Choose?
 
When building a custom connector that integrates an external system (such as Workday, a SaaS application, or a custom database) with Microsoft Purview, your connector must authenticate to **two separate systems**:
 
- **Purview** — to push metadata, entities, lineage, and classifications into the Data Map.
 
- **The external data source** — to extract metadata from the source system (e.g., Workday’s API).
 
Each system has its own authentication mechanism, and they are completely independent of each other. This means you will almost always need a **dual authentication pattern**:
 
┌──────────────────────────────────────────────────────────────────┐
│              YOUR CUSTOM CONNECTOR (Azure Functions)              │
│                                                                  │
│  ┌──────────────────────────┐  ┌──────────────────────────────┐ │
│  │  Auth to Data Source      │  │  Auth to Purview              │ │
│  │                           │  │                               │ │
│  │  Workday: OAuth 2.0 or   │  │  Option A: Managed Identity   │ │
│  │    WS-Security via SOAP   │  │    (preferred in Azure)       │ │
│  │  SQL Server: SQL Auth or  │  │                               │ │
│  │    Windows Auth           │  │  Option B: Service Principal  │ │
│  │  SaaS APIs: API keys or   │  │    (works everywhere)         │ │
│  │    OAuth 2.0              │  │                               │ │
│  │                           │  │                               │ │
│  │  Credentials stored in    │  │  Managed Identity = no        │ │
│  │  Azure Key Vault          │  │  secrets to manage            │ │
│  └──────────┬───────────────┘  └──────────────┬───────────────┘ │
│             │                                  │                 │
└─────────────┼──────────────────────────────────┼─────────────────┘
              │                                  │
              ▼                                  ▼
     ┌─────────────────┐              ┌─────────────────────┐
     │  Workday / SQL / │              │  Purview Data Map    │
     │  SaaS Source      │              │  (Atlas v2 REST API) │
     └─────────────────┘              └─────────────────────┘
 
**Decision guide — Purview-side authentication:**
 
| Scenario | Recommended Auth | Why |
| --- | --- | --- |
| Production workload running in Azure (Azure Functions, VM, Container Apps) | **Managed Identity** (Section 3.1) | No secrets to manage, rotate, or risk leaking. Azure handles everything. Simplest setup (3 steps). |
| Local development or testing | **Service Principal** (Section 3.2) | Managed Identity is not available outside Azure. Service Principal with a .env file is the standard dev approach. |
| Non-Azure hosted environment (on-prem server, AWS, GCP) | **Service Principal** (Section 3.2) | Managed Identity only works in Azure. Service Principal is the only option. |
| Mixed (dev locally, deploy to Azure) | **Both** — use DefaultAzureCredential (Section 3.3) | DefaultAzureCredential automatically uses Managed Identity in Azure and falls back to Service Principal locally. One code path for both. |
 
**Decision guide — Data source-side authentication (e.g., Workday):**
 
Your external data source will always require its own credentials, regardless of how you authenticate to Purview. These credentials should always be stored in **Azure Key Vault** and retrieved at runtime. Common patterns:
 
| Data Source | Typical Auth Method | Credentials to Store in Key Vault |
| --- | --- | --- |
| **Workday** | OAuth 2.0 (REST API) or WS-Security username/password (SOAP/RaaS) | Client ID + Client Secret (OAuth) or Integration System User credentials (SOAP) |
| **SQL Server** | SQL Authentication or Windows/AD Auth | Username + password, or connection string |
| **SaaS APIs** (Salesforce, ServiceNow, etc.) | OAuth 2.0 or API keys | Client ID + Client Secret, or API key |
| **On-prem file systems** | Network credentials or service account | Username + password |
 
**Key takeaway:** Managed Identity eliminates secrets for the **Purview side** of the connection, but you will still need to manage credentials for the **data source side** (e.g., Workday). Azure Key Vault is the recommended store for all data source credentials.
 
#### Native vs. Custom Connectors: Understanding the Authentication Model
 
A common misconception is that natively supported data sources (like Salesforce, Azure SQL, or Amazon S3) do not require dual authentication. In reality, **dual authentication is always required** — even for native connectors. The difference is who manages each side of it.
 
**How native connectors work (e.g., Salesforce):**
 
Purview’s built-in Salesforce connector still needs Salesforce-specific credentials (consumer key, consumer secret, username, password + security token) to authenticate and scan. The supported authentication type for Salesforce is Consumer Key authentication, with the password and consumer secret stored in Azure Key Vault. The authentication chain is:
 
Purview's Managed Identity → Azure Key Vault → retrieves Salesforce creds → Salesforce REST API
 
Purview handles both sides of the authentication for you behind the scenes — you configure it through the portal UI, not through code.
 
**How custom connectors work (e.g., Workday):**
 
Because Workday is not a natively supported Purview data source, you write the connector code yourself. Your code explicitly handles both authentication flows — using Managed Identity to authenticate to Purview, and using Key Vault to retrieve and use Workday credentials:
 
Your Code (Managed Identity) → Purview Data Map API
Your Code (Managed Identity) → Key Vault → retrieves Workday creds → Workday API
 
**Side-by-side comparison:**
 
|  | Native Connector (e.g., Salesforce) | Custom Connector (e.g., Workday) |
| --- | --- | --- |
| **Connector code** | Built into Purview — no code required | You write it (Python, deployed to Azure Functions) |
| **Auth to Purview Data Map** | Handled automatically by Purview internally | Your code uses Managed Identity or Service Principal |
| **Auth to data source** | Salesforce consumer key credentials stored in Key Vault | Workday OAuth/SOAP credentials stored in Key Vault |
| **Who retrieves Key Vault secrets** | Purview’s own Managed Identity | Your code’s Managed Identity |
| **Where scans are configured** | Purview portal UI (Data Map > Sources > New Scan) | Your code / Azure Functions triggers |
| **Scan scheduling** | Purview portal (built-in scheduler) | Azure Functions Timer Trigger (cron schedule) |
| **Lineage** | Automatically generated (if supported for the source) | You build it explicitly via the Atlas v2 API |
| **Setup complexity** | Low — portal-driven configuration | Medium to High — requires development |
| **Flexibility** | Limited to what the built-in connector supports | Full control over metadata, lineage, and transformations |
 
**When to use native connectors:**
 
- Your data source is on Purview’s supported sources list
 
- The built-in connector extracts the metadata you need (schemas, tables, columns, etc.)
 
- The built-in lineage support is sufficient for your requirements
 
- You want minimal development effort
 
**When to build a custom connector:**
 
- Your data source is not natively supported (e.g., Workday, custom databases, proprietary systems)
 
- The native connector exists but does not extract the metadata you need (e.g., Salesforce Data Cloud is not covered by the standard Salesforce connector)
 
- You need custom lineage that the native connector does not provide
 
- You need to transform or enrich metadata before pushing it to Purview
 
- You need to integrate metadata from multiple sources into a unified lineage graph
 
**Important note on Salesforce specifically:** While Purview has a native Salesforce connector, it only covers standard Salesforce objects. Salesforce Data Cloud, custom objects with complex relationships, or Salesforce-to-other-system lineage would require a custom connector approach — the same dual-auth pattern described in this document.
 
#### Can You Build a Custom Connector for a Natively Supported Source?
 
Yes — absolutely. There is nothing preventing you from building a custom connector for any data source, including those that already have native Purview connectors (like Salesforce, Azure SQL, or Amazon S3). Purview’s Atlas v2 APIs accept entities, lineage, and classifications regardless of where the metadata originates. Purview does not enforce any restriction that says “this source already has a native connector, so you cannot push custom metadata for it.”
 
**Reasons you might build a custom connector for a natively supported source:**
 
- **Richer metadata** — The native connector extracts a fixed set of technical metadata (object schemas, column names, data types). If you need additional context such as record counts, field-level usage statistics, last-modified timestamps, or custom object relationships, a custom connector can extract and push that.
 
- **Cross-system lineage** — Native connectors only capture lineage within their own scope. If you need to show data flowing from Salesforce → a middleware ETL → a data warehouse → a BI tool, only a custom connector can create that full end-to-end lineage chain.
 
- **Metadata enrichment at ingestion** — A custom connector can automatically apply business metadata, classifications, labels, and glossary terms during the ingestion process, rather than requiring a separate manual step in the Purview portal after scanning.
 
- **Unsupported sub-features** — Some native connectors do not cover the full breadth of a platform. For example, the Salesforce native connector covers standard Salesforce objects but does not cover Salesforce Data Cloud.
 
- **Custom transformation logic** — If you need to filter, transform, rename, or restructure metadata before it enters Purview (e.g., applying naming conventions, merging duplicate entities, mapping source terminology to your organization’s data glossary), a custom connector gives you full control.
 
#### Hybrid Approach: Native + Custom Connectors Working Together
 
You do not have to choose one or the other — native and custom connectors can work side by side for the same data source. This hybrid approach lets you get the best of both worlds:
 
┌─────────────────────────────────────────────────────────────────┐
│                     SALESFORCE IN PURVIEW                        │
│                                                                 │
│  ┌─────────────────────────┐  ┌──────────────────────────────┐ │
│  │  Native Connector        │  │  Custom Connector             │ │
│  │  (Built into Purview)    │  │  (Your Azure Function)        │ │
│  │                          │  │                               │ │
│  │  ✓ Standard object       │  │  ✓ Cross-system lineage       │ │
│  │    schemas (automatic)   │  │    (SF → ETL → Warehouse)     │ │
│  │  ✓ Column-level metadata │  │  ✓ Business metadata          │ │
│  │  ✓ Basic classifications │  │    (data owner, quality score)│ │
│  │  ✓ Scheduled via portal  │  │  ✓ Record counts & statistics │ │
│  │                          │  │  ✓ Data Cloud objects          │ │
│  │  Runs on Purview's       │  │  ✓ Custom classifications     │ │
│  │  schedule (portal UI)    │  │                               │ │
│  │                          │  │  Runs on Azure Functions      │ │
│  │                          │  │  schedule (Timer Trigger)     │ │
│  └────────────┬─────────────┘  └──────────────┬───────────────┘ │
│               │                                │                │
│               └────────────┬───────────────────┘                │
│                            ▼                                    │
│               ┌─────────────────────────┐                       │
│               │  Purview Data Map        │                       │
│               │  (Unified catalog view)  │                       │
│               └─────────────────────────┘                       │
└─────────────────────────────────────────────────────────────────┘
 
**How the hybrid approach works in practice:**
 
- **Native connector** handles the baseline: Register Salesforce in the Purview portal, configure a scan with Consumer Key authentication, and schedule it to run nightly. This automatically discovers and catalogs all standard Salesforce objects, columns, and basic metadata.
 
- **Custom connector** supplements with everything else: Your Azure Function runs on its own schedule (e.g., after the native scan completes) and uses the Salesforce REST API to extract additional metadata — record counts, field usage stats, Data Cloud objects, or custom relationship data. It then pushes this enriched metadata into Purview via the Atlas v2 API, attaching it to the entities the native connector already created (matching by qualifiedName).
 
- **Custom connector** adds lineage: Your code creates Process entities that link Salesforce objects to downstream systems (data warehouses, BI tools, other SaaS platforms), providing the end-to-end lineage view that native connectors cannot.
 
**Key consideration:** When both connectors write to the same entities, the qualifiedName must match exactly. The native Salesforce connector uses a specific naming convention for entities it creates. Your custom connector should reference those same qualifiedName values when attaching business metadata, classifications, or lineage — rather than creating duplicate entities. You can discover the native connector’s naming convention by running a native scan first and then querying the Purview API to inspect the resulting entities.
 
### 3.1 Recommended for Production: Managed Identity (Purview-Side Auth)
 
Managed Identity is the simplest and most secure way to authenticate your connector to Purview when running in Azure. There are no client secrets to create, store, rotate, or risk leaking — Azure handles the entire token lifecycle automatically.
 
**Setup (3 steps):**
 
- **Enable System-Assigned Managed Identity** on your Azure resource:
 
- Navigate to your Azure Function App (or VM, or Container App) in the Azure portal.
 
- Select **Identity** under **Settings** in the left menu.
 
- On the **System assigned** tab, set **Status** to **On**.
 
- Select **Save**. Azure creates an identity with the same name as your resource.
 
- **Assign Purview Data Plane Roles** to the managed identity:
 
- Go to the Purview governance portal: https://web.purview.azure.com/resource/
 
- Select **Data Map** > **Collections** > your **root collection** > **Role assignments** tab.
 
- Assign **Data Curator** (for entity/lineage CRUD) and **Data Source Administrator** (for source registration).
 
- When searching for the identity, type the **name of your Azure resource** (e.g., your Function App name). Select it from the results and confirm.
 
- **Use** DefaultAzureCredential **in your code** — it automatically detects the managed identity:
 
- from azure.identity import DefaultAzureCredential
 
- credential = DefaultAzureCredential() token = credential.get_token(“https://purview.azure.net/.default”) headers = {“Authorization”: f”Bearer {token.token}“} # Use headers for all Purview API calls — no secrets anywhere in your code
 
**Alternatively**, you can use a **User-Assigned Managed Identity (UAMI)**, which is created as a standalone Azure resource and can be shared across multiple Azure services. This is useful when multiple Azure Functions or services need the same Purview permissions. To use a UAMI:
 
- Create a User-Assigned Managed Identity in the Azure portal (search for “Managed Identities” > **+ Create**).
 
- Attach it to your Function App: Function App > **Identity** > **User assigned** tab > **+ Add**.
 
- Assign Purview roles to the UAMI (same process as above, search by the UAMI’s name).
 
- In code, specify the UAMI’s client ID:
 
- from azure.identity import DefaultAzureCredential
 
- credential = DefaultAzureCredential( managed_identity_client_id=“your-uami-client-id” )
 
**Managed Identity comparison:**
 
| Type | Created As | Lifecycle | Shared Across Resources | Best For |
| --- | --- | --- | --- | --- |
| System-Assigned | Automatically with the Azure resource | Deleted when the resource is deleted | No — tied to one resource | Simple single-function deployments |
| User-Assigned | Standalone Azure resource | Independent — you manage it | Yes — attach to multiple resources | Multiple Functions or services needing the same Purview access |
 
### 3.2 Service Principal (Purview-Side Auth — For Dev or Non-Azure Environments)
 
Use a Service Principal when Managed Identity is not available: local development, non-Azure servers, or CI/CD pipelines. Microsoft recommends creating a **new, dedicated** service principal specifically for Purview API calls rather than reusing an existing one, as reusing existing service principals has a high rate of failure.
 
#### Step 1: Register an Application in Microsoft Entra ID
 
- Sign in to the **Azure portal** (https://portal.azure.com).
 
- In the top search bar, search for and select **Microsoft Entra ID**.
 
- In the left navigation pane, select **App registrations**.
 
- Select **+ New registration** at the top of the page.
 
- On the **Register an application** page, fill in the following:
 
- **Name**: Enter a descriptive name for your connector, e.g., purview-custom-connector-api.
 
- **Supported account types**: Select **“Accounts in this organizational directory only ({Your Tenant Name} only — Single tenant)”**. This is the most common and secure choice for internal API access.
 
- **Redirect URI (optional)**: Select **Web** from the dropdown and enter a placeholder URL such as https://localhost. This value is not used for client-credential flows but must be set to **Web** (not “Single-Page Application”) to avoid cross-origin token errors.
 
- Select **Register**.
 
- On the resulting application overview page, **copy and save** the following two values — you will need them later:
 
- **Application (client) ID** — This is your AZURE_CLIENT_ID
 
- **Directory (tenant) ID** — This is your AZURE_TENANT_ID
 
#### Step 2: Create a Client Secret
 
- From your newly registered application’s page in the Azure portal, select **Certificates ****&**** secrets** in the left navigation pane.
 
- Select the **Client secrets** tab, then select **+ New client secret**.
 
- Enter a **Description** (e.g., purview-connector-secret) and choose an **Expiry** period. For production, a 12- or 24-month expiry is common; you will need to rotate the secret before it expires.
 
- Select **Add**.
 
- **Immediately copy the secret Value** (the string in the **Value** column). This is your AZURE_CLIENT_SECRET. **Important:** You will not be able to view this value again after you navigate away from this page. If you lose it, you must create a new secret.
 
**Security best practice:** Do not store the client secret in source code or config files. Instead, store it in **Azure Key Vault** and reference it from your application at runtime. For local development, use a .env file that is excluded from version control via .gitignore.
 
#### Step 3: Assign Purview Data Plane Roles
 
The service principal must be granted specific roles within Purview to interact with the Data Map APIs. These roles are assigned through the Purview governance portal (not through Azure IAM/RBAC on the Purview resource — those are control-plane roles only).
 
**Navigate to the Purview governance portal:**
 
- Go to https://web.purview.azure.com/resource/ and sign in.
 
- Select **Data Map** in the left menu.
 
- Select **Collections**.
 
- Select the **root collection** (the top-level collection that has the same name as your Purview account). You can also assign roles to a sub-collection, but note that APIs will be scoped to that collection and its children only.
 
- Select the **Role assignments** tab.
 
**Assign the required roles to your service principal:**
 
| Role | Purpose | Required For |
| --- | --- | --- |
| **Data Curator** | Read/write entities, lineage, classifications, business metadata | Entity creation, lineage building, metadata management (this solution’s primary use case) |
| **Data Source Administrator** | Register and manage data sources, trigger scans | Registering custom sources and managing scan configurations |
| **Collection Admin** | Manage collections, assign roles, access Account and Metadata Policy data planes | Only needed if the connector must manage collections or assign roles programmatically |
| **Data Reader** | Read-only access to catalog entities | Only needed for read-only integrations |
 
**For this custom connector solution, you need at minimum: Data Curator + Data Source Administrator.**
 
To add the role assignment:
 
- Under the **Role assignments** tab, find the role you want to assign (e.g., Data Curator).
 
- Select the **edit** (pencil) icon next to the role.
 
- In the search box, type the **name** of your App Registration (e.g., purview-custom-connector-api).
 
- Select the service principal from the results and confirm the assignment.
 
- Repeat for each required role.
 
**Note:** Only users who are **Collection Admins** on the target collection can assign data plane roles. If you cannot see the role assignment options, ask your Purview administrator for access.
 
#### Step 4: Store Credentials Securely
 
For **local development**, create a .env file in your project root (and add it to .gitignore):
 
# .env — DO NOT COMMIT TO SOURCE CONTROL
AZURE_TENANT_ID=12a345bc-67d1-ef89-abcd-efg12345abcde
AZURE_CLIENT_ID=a1234bcd-5678-9012-abcd-abcd1234abcd
AZURE_CLIENT_SECRET=your-client-secret-value-here
PURVIEW_ACCOUNT_NAME=my-purview-account
 
For **production deployments**, store the client secret in **Azure Key Vault**:
 
- Navigate to your Azure Key Vault in the Azure portal (or create one if needed).
 
- Select **Settings ****>**** Secrets ****>**** + Generate/Import**.
 
- Set the **Name** (e.g., purview-connector-client-secret) and paste the client secret as the **Value**.
 
- Select **Create**.
 
- Grant your Azure Function (or compute resource) access to read the secret via Key Vault access policies or RBAC.
 
**Summary of required environment variables:**
 
| Variable | Description | Where to Find |
| --- | --- | --- |
| AZURE_TENANT_ID | Your Microsoft Entra ID (Azure AD) tenant ID | App Registration > Overview, or search “Tenant Properties” in the Azure portal |
| AZURE_CLIENT_ID | The Application (client) ID of your registered app | App Registration > Overview |
| AZURE_CLIENT_SECRET | The client secret value | App Registration > Certificates & secrets (only visible at creation time) |
| PURVIEW_ACCOUNT_NAME | Your Purview account name (without .purview.azure.com) | Azure portal > Your Purview resource > Overview |
 
#### Step 5: Verify Authentication (Test the Token)
 
Before writing connector code, verify that your service principal can successfully acquire a token and reach the Purview API.
 
**Using PowerShell:**
 
$tenantID = "12a345bc-67d1-ef89-abcd-efg12345abcde"
$clientID = "a1234bcd-5678-9012-abcd-abcd1234abcd"
$clientSecret = "your-client-secret-value"
 
$url = "https://login.microsoftonline.com/$tenantID/oauth2/token"
$params = @{
    client_id     = $clientID
    client_secret = $clientSecret
    grant_type    = "client_credentials"
    resource      = "https://purview.azure.net"
}
 
$response = Invoke-WebRequest $url -Method Post -Body $params -UseBasicParsing | ConvertFrom-Json
Write-Host "Token acquired successfully. Expires in $($response.expires_in) seconds."
 
# Test the token against the Purview API
$headers = @{ Authorization = "Bearer $($response.access_token)" }
$testUrl = "https://my-purview-account.purview.azure.com/datamap/api/atlas/v2/types/typedefs?api-version=2023-09-01"
$testResponse = Invoke-WebRequest $testUrl -Headers $headers -UseBasicParsing
Write-Host "API call successful. Status: $($testResponse.StatusCode)"
 
**Using Python:**
 
import requests
import os
 
tenant_id = os.environ["AZURE_TENANT_ID"]
client_id = os.environ["AZURE_CLIENT_ID"]
client_secret = os.environ["AZURE_CLIENT_SECRET"]
account_name = os.environ["PURVIEW_ACCOUNT_NAME"]
 
# Acquire token
token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
token_response = requests.post(token_url, data={
    "client_id": client_id,
    "client_secret": client_secret,
    "grant_type": "client_credentials",
    "resource": "https://purview.azure.net",
})
token_response.raise_for_status()
access_token = token_response.json()["access_token"]
print("Token acquired successfully.")
 
# Test API call — list type definitions
headers = {"Authorization": f"Bearer {access_token}"}
test_url = f"https://{account_name}.purview.azure.com/datamap/api/atlas/v2/types/typedefs"
api_response = requests.get(test_url, headers=headers)
api_response.raise_for_status()
print(f"API call successful. Status: {api_response.status_code}")
print(f"Found {len(api_response.json().get('entityDefs', []))} entity type definitions.")
 
**Common errors and troubleshooting:**
 
| Error | Cause | Fix |
| --- | --- | --- |
| AADSTS7000215: Invalid client secret | Client secret is incorrect or expired | Create a new client secret in App Registration > Certificates & secrets |
| AADSTS700016: Application not found in tenant | Client ID is wrong or app was deleted | Verify AZURE_CLIENT_ID matches the Application (client) ID |
| 403 Forbidden on API calls | Token is valid but service principal lacks Purview roles | Assign Data Curator and/or Data Source Administrator roles in the Purview governance portal |
| Cross-origin token redemption is permitted only for 'Single-Page Application' | Redirect URI type is set to SPA instead of Web | In App Registration > Authentication, change the redirect URI type to **Web** |
| 401 Unauthorized on API calls | Token expired or wrong audience/resource | Ensure resource parameter is https://purview.azure.net (not .default); acquire a fresh token |
 
### 3.3 Data Source Authentication (e.g., Workday)
 
Regardless of how you authenticate to Purview (Managed Identity or Service Principal), you will need separate credentials to connect to your external data source. These credentials are specific to the source system and have nothing to do with Azure.
 
#### Workday Authentication
 
Workday exposes metadata through two primary APIs:
 
**Option A: Workday REST API (OAuth 2.0)** — Recommended for newer integrations.
 
- In your Workday tenant, register an **API Client for Integrations** (Workday > Register API Client).
 
- Note the **Client ID** and generate a **Client Secret** (or use a refresh token flow).
 
- Create an **Integration System User (ISU)** in Workday with the minimum required security groups to read the metadata you need (e.g., worker data, organizational structures, custom reports).
 
- Store the Client ID, Client Secret, and ISU credentials in **Azure Key Vault**.
 
**Option B: Workday SOAP API / Reports-as-a-Service (RaaS)** — Common for established integrations.
 
- Create an **Integration System User (ISU)** in Workday with appropriate security group access.
 
- The ISU authenticates via WS-Security (username/password) over SOAP.
 
- Store the ISU username and password in **Azure Key Vault**.
 
**Retrieving Workday credentials at runtime:**
 
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
 
# Authenticate to Key Vault using Managed Identity (no secrets needed for this step)
credential = DefaultAzureCredential()
kv_client = SecretClient(
    vault_url="https://my-keyvault.vault.azure.net/",
    credential=credential
)
 
# Retrieve Workday credentials from Key Vault
workday_client_id = kv_client.get_secret("workday-client-id").value
workday_client_secret = kv_client.get_secret("workday-client-secret").value
workday_isu_username = kv_client.get_secret("workday-isu-username").value
workday_isu_password = kv_client.get_secret("workday-isu-password").value
workday_tenant_url = kv_client.get_secret("workday-tenant-url").value
 
#### Salesforce Authentication
 
Salesforce supports OAuth 2.0 for server-to-server API access. The recommended flow for a custom Purview connector is the **Client Credentials Flow**, which is designed specifically for server-to-server integrations with no interactive user login.
 
**⚠️ ****IMPORTANT — Connected Apps retired for new integrations (Spring ’26):** As of the Salesforce Spring ’26 release, the creation of new **Connected Apps is disabled by default in all orgs**. New integrations must be created as **External Client Apps (ECAs)** instead (Setup > External Client Apps > New External Client App). Existing Connected Apps continue to function, but Salesforce has announced End-of-Support is coming and recommends migrating using the Connected App → External Client App migration tool. ECAs fully support the OAuth 2.0 Client Credentials Flow used by this connector; the configuration steps are equivalent (enable OAuth settings, enable Client Credentials Flow, assign a Run As integration user, retrieve Consumer Key/Secret). Note that ECAs do **not** support the legacy Username-Password flow. The steps below describe the ECA-equivalent setup; if you are maintaining an existing Connected App-based integration it will continue to work unchanged.
 
**Prerequisites in Salesforce:**
 
- **Create an External Client App** in Salesforce (or a Connected App in orgs where legacy creation is still enabled):
 
- Navigate to **Setup** > **External Client Apps** > **New External Client App** (legacy path: **Setup** > **App Manager** > **New Connected App**).
 
- Provide a name (e.g., Purview Metadata Connector) and contact email.
 
- Under **API (Enable OAuth Settings)**, check **Enable OAuth Settings**.
 
- Set the **Callback URL** to https://localhost (not used for client credentials, but required).
 
- Under **Selected OAuth Scopes**, add **Manage user data via APIs (api)**.
 
- Check **Enable Client Credentials Flow** and confirm the alert.
 
- Click **Save**, then **Continue**.
 
- **Configure the Run As User:**
 
- From the Connected App page, click **Manage** > **Edit Policies**.
 
- Under **Client Credentials Flow**, select a **Run As** user. This should be a dedicated **Integration User** with the **Salesforce Integration** user license and an API-only profile.
 
- Assign a **Permission Set** to the integration user that grants read access to the objects you need to catalog (e.g., Account, Contact, Opportunity, and any custom objects).
 
- Click **Save**.
 
- **Retrieve Consumer Key and Consumer Secret:**
 
- From the Connected App page, click **Manage Consumer Details**.
 
- Copy the **Consumer Key** (this is your client_id) and **Consumer Secret** (this is your client_secret).
 
- Store both values in **Azure Key Vault**.
 
- **Note your Salesforce domain:**
 
- Your token endpoint will be: https://{your-domain}.my.salesforce.com/services/oauth2/token
 
- For sandbox environments, use: https://{your-domain}--{sandbox-name}.sandbox.my.salesforce.com/services/oauth2/token
 
- Store the domain URL in **Azure Key Vault**.
 
**Secrets to store in Azure Key Vault:**
 
| Secret Name | Description | Example |
| --- | --- | --- |
| salesforce-consumer-key | Consumer Key from the Connected App | 3MVG9... |
| salesforce-consumer-secret | Consumer Secret from the Connected App | E8B1C... |
| salesforce-domain-url | Your Salesforce My Domain URL | https://mycompany.my.salesforce.com |
 
**Retrieving Salesforce credentials and authenticating at runtime:**
 
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import requests
 
# Authenticate to Key Vault using Managed Identity
credential = DefaultAzureCredential()
kv_client = SecretClient(
    vault_url="https://my-keyvault.vault.azure.net/",
    credential=credential
)
 
# Retrieve Salesforce credentials from Key Vault
sf_consumer_key = kv_client.get_secret("salesforce-consumer-key").value
sf_consumer_secret = kv_client.get_secret("salesforce-consumer-secret").value
sf_domain_url = kv_client.get_secret("salesforce-domain-url").value
 
# Authenticate to Salesforce using OAuth 2.0 Client Credentials Flow
sf_token_url = f"{sf_domain_url}/services/oauth2/token"
sf_token_response = requests.post(sf_token_url, data={
    "grant_type": "client_credentials",
    "client_id": sf_consumer_key,
    "client_secret": sf_consumer_secret,
})
sf_token_response.raise_for_status()
sf_token_data = sf_token_response.json()
 
sf_access_token = sf_token_data["access_token"]
sf_instance_url = sf_token_data["instance_url"]  # e.g., https://mycompany.my.salesforce.com
sf_headers = {"Authorization": f"Bearer {sf_access_token}"}
 
# Now use sf_headers and sf_instance_url for all Salesforce REST API calls
# e.g., GET {sf_instance_url}/services/data/v66.0/sobjects/ to list all objects
# (v66.0 = Spring '26; check your org's supported versions at /services/data/)
 
**Key differences from Workday authentication:**
 
|  | Salesforce | Workday |
| --- | --- | --- |
| OAuth flow | Client Credentials (grant_type=client_credentials) | Client Credentials or Authorization Code |
| Credentials needed | Consumer Key + Consumer Secret | Client ID + Client Secret (or ISU username/password for SOAP) |
| Token endpoint | {domain}/services/oauth2/token | {domain}/ccx/oauth2/token |
| Response includes | access_token + instance_url | access_token |
| API base URL | Returned in token response as instance_url | Configured per tenant |
| User context | Runs as the configured “Run As” Integration User | Runs as the ISU |
 
**Environment variables for Key Vault access (add to Azure Function App Settings):**
 
| Variable | Description | Example |
| --- | --- | --- |
| KEY_VAULT_URL | URL of your Azure Key Vault | https://my-keyvault.vault.azure.net/ |
| PURVIEW_ACCOUNT_NAME | Your Purview account name | my-purview-account |
 
Note that with Managed Identity + Key Vault, the only configuration value that is not a secret is the Key Vault URL and the Purview account name. All actual credentials live in Key Vault and are retrieved at runtime.
 
#### Oracle NetSuite Authentication
 
NetSuite’s SuiteTalk REST Web Services support two authentication methods: **OAuth 1.0a Token-Based Authentication (TBA)** and **OAuth 2.0**.
 
**⚠️ ****IMPORTANT — TBA deprecation timeline (announced NetSuite 2026.1):** Oracle has announced the phased end of support for Token-Based Authentication. As of the **NetSuite 2027.1 release, new integrations can no longer use TBA** — OAuth 2.0 becomes mandatory for all new builds. Support for **existing** TBA integrations is tentatively scheduled to end in **NetSuite 2028.1**. The SuiteCloud SDK already removed OAuth 1.0/TBA support in version 24.2. **All new Purview connector deployments should use OAuth 2.0**, and existing TBA-based deployments should plan migration before 2027.1.
 
**Recommended: OAuth 2.0 Client Credentials Flow (Machine-to-Machine)**
 
The OAuth 2.0 Client Credentials (M2M) flow is NetSuite’s recommended approach for server-to-server integrations. Unlike Salesforce’s flow, NetSuite requires a **certificate-based signed JWT assertion** rather than a simple client secret:
 
- **Enable features:** Setup > Company > Enable Features > SuiteCloud — enable **REST Web Services** and **OAuth 2.0**.
 
- **Create an Integration Record:** Setup > Integration > Manage Integrations > New. Check **Client Credentials (Machine to Machine) Grant** under OAuth 2.0. Save and note the **Client ID (Consumer Key)** — it is shown only once.
 
- **Generate a certificate key pair** (RSA 4096 with PSS padding, or EC) using OpenSSL: openssl req -new -x509 -newkey rsa:4096 -keyout private.pem -sigopt rsa_padding_mode:pss -sha256 -sigopt rsa_pss_saltlen:64 -out public.pem -nodes -days 365
 
- **Map the certificate:** Setup > Integration > OAuth 2.0 Client Credentials (M2M) Setup > Create New. Select the integration record, the integration user/role, and upload public.pem. Note the **Certificate ID** (used as the JWT kid header).
 
- **Token request:** POST a signed JWT assertion (grant_type=client_credentials, client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer) to https://{account-id}.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token. Access tokens are valid for **60 minutes**; there is no refresh token — request a new access token on expiry.
 
- **Store in Azure Key Vault:** the Client ID, the private key (private.pem contents), and the Certificate ID. NetSuite also provides REST endpoints to rotate certificates programmatically.
 
**Legacy: OAuth 1.0a Token-Based Authentication (existing integrations only)**
 
TBA signs every request with HMAC-SHA256 using four credentials: Consumer Key + Consumer Secret (from the Integration Record) and Token ID + Token Secret (from an Access Token tied to an integration user/role). The requests-oauthlib Python library handles signature generation. If maintaining an existing TBA integration, store all four values in Azure Key Vault — but plan migration to OAuth 2.0 before the 2027.1 cutoff for new-integration support and the tentative 2028.1 end of support for existing integrations.
 
#### Other Data Sources
 
For other data sources, the same pattern applies — store credentials in Azure Key Vault and retrieve them at runtime. The only difference is the type of credentials:
 
# SQL Server — retrieve connection string from Key Vault
sql_connection_string = kv_client.get_secret("sql-connection-string").value
 
# SaaS API — retrieve API key from Key Vault
api_key = kv_client.get_secret("saas-api-key").value
 
# On-prem system — retrieve service account credentials from Key Vault
service_account_user = kv_client.get_secret("onprem-service-user").value
service_account_password = kv_client.get_secret("onprem-service-password").value
 
### 3.4 Complete Dual-Auth Code Pattern
 
The following code demonstrates the recommended pattern for a production connector that authenticates to both Purview and an external data source (using Workday as the example):
 
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyapacheatlas.core import PurviewClient
import os
import requests
 
# ===========================================================================
# STEP 1: Establish base credential (Managed Identity in Azure, or
# Service Principal locally via environment variables).
# DefaultAzureCredential handles both automatically.
# ===========================================================================
credential = DefaultAzureCredential()
 
# ===========================================================================
# STEP 2: Authenticate to Purview (using Managed Identity — no secrets)
# ===========================================================================
# Option A: Using pyapacheatlas with DefaultAzureCredential
# (pyapacheatlas supports passing a credential object directly)
purview_account = os.environ["PURVIEW_ACCOUNT_NAME"]
 
# For direct REST API calls:
purview_token = credential.get_token("https://purview.azure.net/.default")
purview_headers = {
    "Authorization": f"Bearer {purview_token.token}",
    "Content-Type": "application/json",
}
purview_endpoint = f"https://{purview_account}.purview.azure.com/datamap"
 
# ===========================================================================
# STEP 3: Retrieve data source credentials from Azure Key Vault
# (Key Vault access also uses Managed Identity — still no secrets)
# ===========================================================================
kv_url = os.environ.get("KEY_VAULT_URL", "https://my-keyvault.vault.azure.net/")
kv_client = SecretClient(vault_url=kv_url, credential=credential)
 
# Retrieve Workday-specific credentials
workday_base_url = kv_client.get_secret("workday-base-url").value
workday_client_id = kv_client.get_secret("workday-client-id").value
workday_client_secret = kv_client.get_secret("workday-client-secret").value
 
# ===========================================================================
# STEP 4: Authenticate to Workday (OAuth 2.0 example)
# ===========================================================================
workday_token_url = f"{workday_base_url}/ccx/oauth2/token"
workday_token_response = requests.post(workday_token_url, data={
    "client_id": workday_client_id,
    "client_secret": workday_client_secret,
    "grant_type": "client_credentials",
})
workday_token_response.raise_for_status()
workday_access_token = workday_token_response.json()["access_token"]
workday_headers = {"Authorization": f"Bearer {workday_access_token}"}
 
# ===========================================================================
# Now you have:
#   purview_headers  — for all Purview Data Map API calls
#   workday_headers  — for all Workday API calls
# No secrets are stored in code, config files, or environment variables.
# ===========================================================================
 
**For local development** (where Managed Identity is not available), DefaultAzureCredential falls back to environment variables. Set these in a .env file (excluded from source control via .gitignore):
 
# .env — DO NOT COMMIT TO SOURCE CONTROL
# Used by DefaultAzureCredential for Service Principal fallback
AZURE_TENANT_ID=12a345bc-67d1-ef89-abcd-efg12345abcde
AZURE_CLIENT_ID=a1234bcd-5678-9012-abcd-abcd1234abcd
AZURE_CLIENT_SECRET=your-client-secret-value-here
 
# Application config
PURVIEW_ACCOUNT_NAME=my-purview-account
KEY_VAULT_URL=https://my-keyvault.vault.azure.net/
 
# For local dev only — Workday credentials (in production these come from Key Vault)
WORKDAY_BASE_URL=https://wd3-impl-services1.workday.com
WORKDAY_CLIENT_ID=your-workday-client-id
WORKDAY_CLIENT_SECRET=your-workday-client-secret
 
### 3.5 Secret Rotation and Lifecycle Management
 
Client secrets have a defined expiry (6, 12, or 24 months). To avoid connector downtime:
 
- **Set a calendar reminder** 30 days before the secret expires.
 
- **Create a new secret** in the App Registration before the old one expires.
 
- **Update the secret** in Azure Key Vault (or environment variables).
 
- **Verify** the new secret works by running the token test from Step
 
- **Delete the old secret** from the App Registration after confirming the new one is in use.
 
For a fully automated approach, consider using **Azure Key Vault’s secret rotation** with an Azure Function trigger that automatically creates a new client secret and updates the Key Vault entry.
 
## 4. Core Capabilities
 
### 4.1 Custom Type Definitions
 
Before ingesting assets from a custom source, you must define entity types that describe your data assets. Types are defined once and reused across all entities of that kind.
 
**When to create custom types:**
 
- Your data source is not natively supported by Purview
 
- You need additional attributes beyond what built-in types provide
 
- You want to model domain-specific relationships
 
**Type hierarchy:**
 
DataSet (built-in)
  └── custom_database (your type)
        └── custom_schema (your type)
              └── custom_table (your type)
                    └── custom_column (your type)
 
### 4.2 Entity Creation and Metadata Ingestion
 
Entities represent individual data assets (databases, tables, columns, files, etc.) in the Purview catalog. The solution uses bulk entity creation via the /entity/bulk endpoint for efficiency.
 
**Key considerations:**
 
- Every entity requires a globally unique qualifiedName — use a consistent naming convention like {source_type}://{server}/{database}/{schema}/{table}
 
- Entities are upserted (created or updated) based on qualifiedName, so re-running a scan is safe and idempotent
 
- Bulk API supports up to 50 entities per request
 
### 4.3 Custom Lineage
 
Lineage in Purview is modeled as a **Process** entity with inputs (source entities) and outputs (destination entities). The Process entity represents the transformation or movement of data.
 
**Lineage model:**
 
[Source Entity A] ──┐
                    ├──▶ [Process Entity] ──▶ [Destination Entity C]
[Source Entity B] ──┘
 
**Lineage via pyapacheatlas** uses the AtlasProcess class which simplifies the creation of process entities with input/output references.
 
### 4.4 Business Metadata and Classifications
 
Business metadata templates allow you to attach custom key-value attributes to any entity. Classifications provide categorical tagging for sensitivity and data type labeling (e.g., “PII”, “Confidential”, “MICROSOFT.FINANCIAL.CREDIT_CARD_NUMBER”). Note that classifications are distinct from glossary terms — classifications are system-level labels typically used for data sensitivity, while glossary terms are business-level definitions managed in the Purview glossary.
 
**Business metadata** is applied via:
 
POST {endpoint}/api/atlas/v2/entity/guid/{GUID}/businessmetadata?isOverwrite=true
 
**Classifications** are applied via:
 
POST {endpoint}/api/atlas/v2/entity/guid/{GUID}/classifications
 
### 4.4.1 Configuration-Driven Classification Engine
 
The connector examples do not hardcode classification logic. Classification is driven by two files shared across all four connectors:
 
- **classification_rules.json** — the rules file, maintained by data stewards. Adding, modifying, or disabling a classification rule requires a JSON edit and a connector re-run; no Python changes.
 
- **classification_engine.py** — the shared engine, imported by every connector. It loads the rules file and classifies fields at discovery time.
 
**Three rule layers are evaluated; the highest priority match wins:**
 
| Layer | Matches on | Priority |
| --- | --- | --- |
| object_field_rules | Exact source + object + field (e.g., salesforce / Contact / Email) | 50 |
| field_name_patterns | Wildcard on the field name (e.g., *email*, *amount*) | 10 |
| field_type_rules | The source API data type (e.g., email, phone, currency) | 5 |
 
Rules can be disabled without deletion by setting "enabled": false.
 
**Integration pattern** (identical in all four connectors):
 
from classification_engine import ClassificationEngine
 
engine = ClassificationEngine()
result = engine.classify_field("salesforce", "Contact", "Email", "email")
# -> "MICROSOFT.PERSONAL.EMAIL"
 
batch = engine.classify_fields("salesforce", "Contact", discovered_fields)
# -> {"Email": "MICROSOFT.PERSONAL.EMAIL", "Phone": "MICROSOFT.PERSONAL.PHONE_NUMBER", ...}
 
classify_fields() accepts either name_key/type_key or field_name_key/field_type_key keyword arguments — both spellings are supported. The engine ships with a built-in self-test (python classification_engine.py).
 
In the current connector implementations, each field is classified at entity-build time and the resulting classification rides on the entity payload submitted to /entity/bulk — classifications land in the Data Map in the same call that creates the entity, rather than in a separate per-GUID pass.
 
**Important limitation:** this is rule-based classification (field names, types, and object context), not content-based classification (inspecting actual data values). For content-level detection in SaaS sources, complement it with the source system’s own scanning (e.g., Salesforce Shield Data Detect) or Microsoft Defender for Cloud Apps — see the MDCA companion guide.
 
### 4.5 Scan Run Stamping & Rollback
 
Every connector run generates a unique scan run identifier at startup and stamps it on everything the run writes, enabling surgical rollback of a bad run without touching entities written by other runs.
 
**How stamping works:**
 
- A SCAN_RUN_ID (UTC timestamp + random suffix, e.g., 20260716T163729Z-e3697e8e) is generated once per process and logged at startup, so it is captured in Application Insights. Always note the run ID before and after a production run.
 
- Every custom entity type registers a searchable scanRunId string attribute (isIndexable: true).
 
- build_entity() stamps the current run’s ID on every entity it constructs, so the value is written (and overwritten on upsert) with each run.
 
**Rolling back a bad run** — the standalone utility purview_rollback_scan_run.py finds every entity carrying a given run ID via the Purview search API and soft-deletes it:
 
python purview_rollback_scan_run.py --run-id <bad-run-id>            # dry run (default): lists what would be deleted
python purview_rollback_scan_run.py --run-id <bad-run-id> --execute  # deletes, after a typed confirmation of the run ID
 
Deletion in Purview is a soft delete: entities move to DELETED status and disappear from the catalog; a subsequent healthy scan recreates legitimate assets fresh via upsert.
 
**Decision rule:** roll back only when a bad run *created entities that should not exist*. If a bad run merely wrote wrong values onto existing entities, do not roll back — fix the connector and re-run; the upsert on qualifiedName overwrites the bad values (self-healing). The full operational procedure — release tagging, the Timer-trigger kill switch, the rollback decision tree, and the post-rollback validation sequence — is in **Purview_Connector_Setup_Quickstart.md**, Phase 7.
 
## 5. Project Structure
 
purview-custom-api/
├── config/
│   ├── settings.py              # Environment config and constants
│   └── type_definitions.json    # Custom type definitions
├── connectors/
│   ├── base_connector.py        # Abstract base class for all connectors
│   ├── sql_connector.py         # Example: SQL Server connector
│   ├── salesforce_connector.py  # Example: Salesforce connector (custom, via Atlas v2)
│   ├── workday_connector.py     # Example: Workday connector
│   ├── netsuite_connector.py    # Example: Oracle NetSuite connector
│   └── api_connector.py         # Example: Generic REST API source connector
├── services/
│   ├── auth_service.py          # Dual authentication manager (Purview + data sources)
│   ├── keyvault_service.py      # Azure Key Vault credential retrieval
│   ├── entity_service.py        # Entity CRUD operations
│   ├── lineage_service.py       # Lineage creation and management
│   ├── typedef_service.py       # Type definition management
│   └── metadata_service.py      # Business metadata and classifications
├── models/
│   └── entities.py              # Data classes for entity models
├── classification/
│   ├── classification_engine.py # Shared engine: loads rules, classifies fields
│   └── classification_rules.json# Rules file (data stewards maintain this)
├── tools/
│   └── purview_rollback_scan_run.py # Scan-run rollback utility (dry-run by default)
├── main.py                      # Entry point / orchestrator
├── requirements.txt             # Python dependencies
└── README.md                    # Setup and usage instructions
 
## 6. Deployment Recommendations
 
### 6.1 Development and Testing
 
Run locally as Python scripts. Use a Service Principal with credentials stored in a .env file (excluded from source control). Test against a non-production Purview account.
 
### 6.2 Production: Azure Functions
 
#### What Are Azure Functions?
 
Azure Functions is Microsoft’s serverless compute service that lets you run event-driven code without deploying or managing servers. Instead of provisioning a dedicated virtual machine or web server that runs 24/7 (and incurs costs even when idle), Azure Functions executes your code only when it is triggered — by a schedule, an HTTP request, a message on a queue, or dozens of other event types — and then automatically shuts down. Azure handles all infrastructure concerns: provisioning, scaling, patching, and availability.
 
Azure Functions supports Python, C#, JavaScript, PowerShell, Java, and other languages natively. For this Purview connector solution, we use **Python**.
 
**Key characteristics:**
 
- **Serverless** — No servers to provision, patch, or maintain. You deploy your code and Azure runs it.
 
- **Event-driven** — Functions execute in response to triggers (timers, HTTP requests, queue messages, etc.).
 
- **Auto-scaling** — Azure automatically scales the number of running instances up or down based on demand, from zero (when idle) to thousands of concurrent executions.
 
- **Pay-per-use** — On the Consumption plan, you are billed only for the time your code is actively executing, measured in gigabyte-seconds. There is a generous free tier of 1,000,000 executions per month.
 
#### Why Azure Functions for This Solution
 
Azure Functions is the recommended deployment target for the Purview custom connector because it addresses the core operational requirements of a metadata ingestion pipeline:
 
| Requirement | How Azure Functions Addresses It |
| --- | --- |
| **Scheduled execution** | Timer Triggers run the connector on a cron schedule (e.g., nightly at 2 AM) with no external scheduler needed |
| **On-demand execution** | HTTP Triggers provide a URL endpoint to run the connector ad-hoc (e.g., after a data migration) |
| **Cost efficiency** | The connector only runs for minutes per day; serverless billing means you pay only for those minutes, not for 24 hours of idle compute |
| **Credential-free authentication** | Managed Identity (Section 3.1) integrates natively with Azure Functions — no client secrets to store or rotate |
| **Scalability** | If you have multiple data sources, each can be scanned concurrently by separate function instances that scale automatically |
| **Monitoring and alerting** | Built-in integration with Azure Monitor and Application Insights provides logs, metrics, and failure alerts out of the box |
| **Private networking** | Flex Consumption and Premium plans support VNet integration, allowing the connector to reach on-premises or privately networked data sources |
 
#### Azure Functions Hosting Plans
 
Azure Functions offers several hosting plans. The right choice depends on your workload characteristics:
 
| Plan | Best For | Scaling | Cold Start | Networking | Billing |
| --- | --- | --- | --- | --- | --- |
| **Consumption** | Periodic/lightweight connectors that run a few times per day | Automatic, scales to zero | Yes (a few seconds on first invocation) | Public only | Pay-per-execution (free tier: 1M executions/month) |
| **Flex Consumption** | Production workloads needing private networking and faster starts | Automatic with configurable concurrency and instance sizes (2 GB / 4 GB) | Reduced (faster than Consumption) | VNet integration supported | Pay-per-execution with configurable instance memory |
| **Premium (Elastic Premium)** | Latency-sensitive connectors or always-on requirements | Always-warm instances + auto-scale | No cold start (pre-warmed instances) | VNet integration supported | Per-second billing for pre-warmed + active instances |
| **Dedicated (App Service)** | When you already have an App Service Plan and want fixed-cost compute | Manual or auto-scale rules | No cold start | Full VNet support | Fixed monthly cost (App Service Plan pricing) |
 
**Recommendation for this solution:**
 
- **Start with Consumption plan** during development and for low-frequency connectors (e.g., one nightly scan). It is the simplest to set up and costs nothing for low volumes.
 
- **Move to Flex Consumption** for production if you need VNet integration (to reach on-prem or private data sources) or want faster cold starts and larger memory.
 
- **Use Premium** only if the connector must respond to HTTP triggers with sub-second latency or must remain always-warm.
 
#### How Azure Functions Fits the Architecture
 
In the architecture diagram from Section 2, Azure Functions serves as the **Orchestration Layer** — the top-level component that triggers and coordinates connector runs:
 
┌──────────────────────────────────────────────────────────────────────┐
│                    AZURE FUNCTIONS (Orchestration Layer)              │
│                                                                      │
│  ┌────────────────────┐   ┌────────────────────┐                    │
│  │  Timer Trigger       │   │  HTTP Trigger        │                    │
│  │  (Scheduled scans)   │   │  (On-demand scans)   │                    │
│  │  e.g., "0 0 2 * * *" │   │  POST /api/scan      │                    │
│  │  = every day at 2 AM │   │  = ad-hoc execution  │                    │
│  └─────────┬──────────┘   └─────────┬──────────┘                    │
│            │                        │                                │
│            └──────────┬─────────────┘                                │
│                       ▼                                              │
│          ┌────────────────────────┐                                  │
│          │  Connector Orchestrator │                                  │
│          │  (main.py logic)        │                                  │
│          └────────────┬───────────┘                                  │
│                       │                                              │
│  ┌────────────────────┼────────────────────┐                        │
│  ▼                    ▼                    ▼                        │
│  SQL Connector    SaaS Connector    File Connector                  │
│                                                                      │
│  ── Authentication via Managed Identity (no secrets) ──             │
│                                                                      │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  Purview Data Map    │
                    │  (Atlas v2 REST API) │
                    └─────────────────────┘
 
#### Trigger Types Explained
 
**Timer Trigger (scheduled execution):**
 
A Timer Trigger runs your function on a cron-like schedule. This is the primary trigger for the Purview connector — it ensures your catalog stays up to date without manual intervention.
 
# function_app.py — Timer Trigger example
import azure.functions as func
import logging
from purview_custom_api.main import main as run_connector
 
app = func.FunctionApp()
 
@app.timer_trigger(
    schedule="0 0 2 * * *",   # Runs every day at 2:00 AM UTC
    arg_name="timer",
    run_on_startup=False       # Set to True to also run on deployment
)
def purview_scheduled_scan(timer: func.TimerRequest) -> None:
    """Scheduled Purview connector run — scans all configured sources."""
    logging.info("Purview scheduled scan started.")
    try:
        run_connector()
        logging.info("Purview scheduled scan completed successfully.")
    except Exception as e:
        logging.error(f"Purview scheduled scan failed: {e}")
        raise
 
Common cron schedule examples:
 
| Schedule Expression | Meaning |
| --- | --- |
| 0 0 2 * * * | Every day at 2:00 AM |
| 0 0 */6 * * * | Every 6 hours |
| 0 30 9 * * 1-5 | Weekdays at 9:30 AM |
| 0 0 0 1 * * | First day of each month at midnight |
 
**HTTP Trigger (on-demand execution):**
 
An HTTP Trigger exposes your function as a REST API endpoint. Use this for ad-hoc scans or integrations with other systems.
 
# function_app.py — HTTP Trigger example
@app.route(route="scan", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def purview_on_demand_scan(req: func.HttpRequest) -> func.HttpResponse:
    """On-demand Purview connector run — triggered via HTTP POST."""
    logging.info("Purview on-demand scan triggered via HTTP.")
    try:
        # Optionally read parameters from the request body
        body = req.get_json() if req.get_body() else {}
        source_filter = body.get("source", "all")
 
        run_connector(source_filter=source_filter)
        return func.HttpResponse("Scan completed successfully.", status_code=200)
    except Exception as e:
        logging.error(f"On-demand scan failed: {e}")
        return func.HttpResponse(f"Scan failed: {str(e)}", status_code=500)
 
Once deployed, you would call this endpoint like:
 
curl -X POST https://my-function-app.azurewebsites.net/api/scan \
     -H "x-functions-key: your-function-key" \
     -H "Content-Type: application/json" \
     -d '{"source": "sql-prod-01"}'
 
#### Azure Function Project Structure
 
When deployed as an Azure Function, the project structure wraps the core connector library:
 
purview-connector-function/
├── function_app.py               # Function trigger definitions (Timer + HTTP)
├── host.json                     # Azure Functions host configuration
├── local.settings.json           # Local dev settings (NOT committed to source control)
├── requirements.txt              # Python dependencies
│
├── purview_custom_api/           # Core connector library (from Section 5)
│   ├── config/
│   │   ├── settings.py
│   │   └── type_definitions.json
│   ├── connectors/
│   │   ├── base_connector.py
│   │   ├── sql_connector.py
│   │   └── api_connector.py
│   ├── services/
│   │   ├── auth_service.py
│   │   ├── entity_service.py
│   │   ├── lineage_service.py
│   │   ├── typedef_service.py
│   │   └── metadata_service.py
│   ├── models/
│   │   └── entities.py
│   └── main.py                   # Orchestrator logic (called by function triggers)
│
└── tests/                        # Unit and integration tests
    ├── test_connectors.py
    └── test_services.py
 
**Key configuration files:**
 
host.json — Controls runtime behavior:
 
{
  "version": "2.0",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true,
        "excludedTypes": "Request"
      }
    }
  },
  "functionTimeout": "00:10:00"
}
 
local.settings.json — Local development settings (mimics Azure app settings):
 
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AZURE_TENANT_ID": "your-tenant-id",
    "AZURE_CLIENT_ID": "your-client-id",
    "AZURE_CLIENT_SECRET": "your-client-secret",
    "PURVIEW_ACCOUNT_NAME": "your-purview-account"
  }
}
 
requirements.txt:
 
azure-functions
pyapacheatlas>=0.14.0
azure-identity>=1.15.0
requests>=2.31.0
python-dotenv>=1.0.0
 
#### Step-by-Step: Deploying to Azure Functions
 
**Step 1: Create the Function App in Azure**
 
- In the Azure portal, search for **Function App** and select **+ Create**.
 
- Fill in the basics:
 
- **Subscription**: Your Azure subscription
 
- **Resource Group**: Select or create one (e.g., rg-purview-connector)
 
- **Function App name**: A globally unique name (e.g., func-purview-connector-prod)
 
- **Runtime stack**: Python
 
- **Version**: 3.11 or later
 
- **Region**: Same region as your Purview account for lowest latency
 
- **Hosting plan**: Consumption (to start) or Flex Consumption (for VNet/production)
 
- On the **Storage** tab, create or select a storage account (required by Azure Functions for internal state).
 
- On the **Monitoring** tab, enable **Application Insights** for logging and alerting.
 
- Select **Review + create**, then **Create**.
 
**Step 2: Enable Managed Identity**
 
- Navigate to your newly created Function App in the Azure portal.
 
- In the left menu, select **Identity** under **Settings**.
 
- On the **System assigned** tab, set **Status** to **On** and select **Save**.
 
- Copy the **Object ID** — you will use this to assign Purview roles.
 
- In the Purview governance portal, assign **Data Curator** and **Data Source Administrator** roles to this managed identity (same process as Section 3.1, Step 2, searching for the Function App name).
 
**Step 3: Configure Application Settings**
 
- In the Function App, select **Configuration** under **Settings** (or **Environment variables** in the newer portal).
 
- Add the following application settings:
 
- PURVIEW_ACCOUNT_NAME = your Purview account name
 
- (If using Service Principal instead of Managed Identity, also add AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET — but Managed Identity is preferred)
 
- Select **Save**.
 
**Step 4: Deploy Your Code**
 
You can deploy using several methods:
 
Using **Azure Functions Core Tools** (CLI):
 
# Install Azure Functions Core Tools if not already installed
npm install -g azure-functions-core-tools@4
 
# From the project root directory
func azure functionapp publish func-purview-connector-prod
 
Using **VS Code** with the Azure Functions extension:
 
- Install the **Azure Functions** extension in VS Code.
 
- Open the command palette (Ctrl+Shift+P) and select **Azure Functions: Deploy to Function App**.
 
- Select your Function App and confirm.
 
Using **GitHub Actions / Azure DevOps** (CI/CD):
 
Set up a deployment pipeline that automatically deploys on push to main. Azure provides starter templates for both GitHub Actions and Azure DevOps Pipelines.
 
**Step 5: Verify the Deployment**
 
- In the Azure portal, navigate to your Function App.
 
- Select **Functions** in the left menu — you should see your timer and HTTP trigger functions listed.
 
- For the HTTP trigger: select the function, then **Get Function URL**, and test with curl or Postman.
 
- For the Timer trigger: select the function, then **Code + Test** > **Test/Run** to execute manually.
 
- Check **Monitor** > **Logs** or **Application Insights** to confirm successful execution.
 
### 6.3 Alternative: Azure Data Factory
 
If your data movement already uses ADF, you can invoke the custom connector as a **Custom Activity** within an ADF pipeline, running on an Azure Batch pool.
 
### 6.4 Alternative: Azure Container Apps
 
For more complex microservice architectures, or if you need long-running connector jobs that exceed Azure Functions’ timeout limits (default 10 minutes on Consumption), you can containerize the connector and deploy it to Azure Container Apps. This gives you the same serverless scaling benefits with more control over the runtime environment.
 
### 6.5 Deployment Decision Matrix
 
| Factor | Azure Functions (Recommended) | Azure Data Factory | Azure Container Apps |
| --- | --- | --- | --- |
| Best for | Scheduled/event-driven scans, lightweight pipelines | Integration with existing ADF pipelines | Long-running scans, complex multi-service architectures |
| Setup complexity | Low | Medium | Medium-High |
| Cost at low volume | Very low (free tier available) | Medium (pipeline activity costs) | Low (scale to zero) |
| Max execution time | 10 min (Consumption), 60 min (Premium/Flex) | No limit (pipeline orchestration) | No limit |
| Managed Identity | Native support | Native support | Native support |
| VNet support | Flex Consumption / Premium plans | Native | Native |
| Monitoring | Application Insights (built-in) | ADF Monitor (built-in) | Azure Monitor / Log Analytics |
 
## 7. Error Handling and Resilience
 
The connector examples implement the following strategies directly (see the _request_with_retry() helper and the per-object isolation pattern in each connector’s discovery loop):
 
| Scenario | Strategy |
| --- | --- |
| API rate limiting (429) | _request_with_retry() performs exponential backoff with jitter and honors the Retry-After response header — important because source API quotas (e.g., Salesforce daily request limits) are shared org-wide resources |
| Transient server errors (5xx) | Retried by _request_with_retry() with exponential backoff (4 attempts max), then raised |
| Network/connection/timeout errors | Retried with backoff by _request_with_retry(); all HTTP calls carry explicit connect/read timeouts (10 s / 30 s) |
| Failure on a single object during discovery | Per-object try/except isolation: the object is logged (exception class name only — no stack traces or internal identifiers in logs) and skipped; the scan continues and the summary reports skipped objects |
| Unsafe metadata from a source system | _safe_name_component() raises ValueError; the object is rejected loudly and skipped rather than silently polluting the Data Map |
| Partial batch failure | Log failed entities, continue with remaining; retry failed batch |
| Authentication token expiry | DefaultAzureCredential auto-refreshes Purview tokens; NetSuite ensure_token() re-authenticates proactively 5 minutes before the 60-minute token expiry |
| Duplicate entities | Upsert behavior on qualifiedName prevents duplicates and makes re-runs after partial failures idempotent |
| Invalid type definitions | Validate JSON schema before submission; log detailed errors |
 
## 8. Security Considerations
 
A full security code review of the four connector examples — including an assessment against the OWASP GenAI Security Project’s Top 10 for LLM Applications (2025) — is documented in the companion **Purview Connector Security Review** document. All medium and low findings from that review have been remediated in the connector code (v1.1 of the review records the status of each). The subsections below summarize the baseline practices and the controls now built into the connectors.
 
### 8.1 Baseline Practices
 
- Use Managed Identity when running in Azure to eliminate secret management for the Purview side of the connection
 
- Store all data source credentials (Workday, Salesforce, NetSuite, SQL Server, etc.) in Azure Key Vault; never hardcode secrets in code, config files, or environment variables
 
- Grant the Azure Function’s Managed Identity only **Key Vault Secrets User** role on the Key Vault (least privilege for reading secrets)
 
- Assign minimum required Purview roles — Data Curator + Data Source Administrator for this solution; avoid Collection Admin unless programmatic collection management is needed. **Scope these role assignments to a dedicated collection per source** rather than the root collection, and use **one Managed Identity per connector** so blast radius and audit trails stay separable (OWASP LLM06 — see Section 8.3)
 
- Enable diagnostic logging on the Purview account for audit trails of all API calls
 
- Use private endpoints for Purview if the solution runs within a VNet (available on Flex Consumption and Premium Azure Functions plans)
 
- Rotate data source credentials (e.g., NetSuite certificates, which expire after a maximum of 2 years) on a regular schedule and update them in Key Vault; use Key Vault secret expiry notifications to automate reminders
 
- Exclude .env files and local.settings.json from version control via .gitignore to prevent accidental credential leaks during development
 
- For CI/CD pipelines, use Azure DevOps service connections or GitHub Actions secrets — never store credentials in pipeline YAML files
 
- For production deployments, install dependencies from a hash-pinned lock file (pip-compile –generate-hashes + pip install –require-hashes) and run SCA scanning against it in CI, so a compromised or hijacked package version cannot be silently pulled in
 
### 8.2 Controls Implemented in the Connector Code
 
The connector examples ship with the following security controls, added as remediations from the security review:
 
- **Source-metadata validation and sanitization:** _safe_name_component() validates every source-derived name (object, record type, field) against a strict character allow-list before it is used in a qualifiedName — a crafted name can no longer corrupt the catalog hierarchy or spoof another asset’s identity. _sanitize_text() strips control characters, collapses newlines, and caps labels (256 chars) and descriptions (2,000 chars) before ingestion. Unsafe metadata fails loudly (ValueError) and the object is skipped.
 
- **Provenance tagging:** every connector-written entity carries a sourceOfTruth: “connector:” attribute so downstream consumers — human curators and AI features alike — can distinguish machine-synced text from human-curated text.
 
- **Purview endpoint allow-list:** the PURVIEW_ENDPOINT override is validated by _validate_purview_endpoint() — HTTPS only, and only recognized Microsoft Purview hosts (*.purview.azure.com, api.purview-service.microsoft.com, and the tenant-specific *-api.purview-service.microsoft.com form). This prevents an attacker who can tamper with runtime environment settings from redirecting the metadata stream (including PII classifications and record counts) to an untrusted destination.
 
- **Business-metadata gating:** governance metadata (quality scores, owners, validation dates) is applied only when the APPLY_BUSINESS_METADATA environment variable is explicitly set to true, and the example values are marked as REPLACE-ME placeholders. This prevents fabricated governance signals from reaching a production catalog. PII/financial classifications are not gated — they derive from real field discovery.
 
- **Injection guards:** SOQL identifiers are validated and SuiteQL tables resolved through an explicit allow-list map before query construction.
 
- **Data minimization:** the Workday record-count call uses limit=0 so only the total is returned — a real worker record (name, email, compensation) never transits memory when only a count is needed. Salesforce and NetSuite counts are aggregate-only queries.
 
- **Optional Key Vault signing (NetSuite):** an alternative implementation signs the OAuth 2.0 JWT assertion via the Azure Key Vault cryptography client, so the private key is stored as a Key Vault key and never enters process memory.
 
**Connector environment variables added by these controls:**
 
| Variable | Default | Purpose |
| --- | --- | --- |
| PURVIEW_ENDPOINT | (unset) | Optional override for the new unified portal endpoint; validated against the trusted-host allow-list |
| APPLY_BUSINESS_METADATA | false | Gate for Step 6 business metadata; enable only after replacing placeholder values with computed ones |
 
### 8.3 AI / GenAI Considerations (OWASP LLM Top 10)
 
The connectors contain no LLM calls, but they are a **data pipeline into an AI-consumed knowledge base**: the Purview catalog is surfaced through Copilot experiences and AI-powered search, and is a common grounding source for internal RAG assistants. Two OWASP GenAI risks therefore apply at the data-supply boundary and are mitigated by the controls in Section 8.2:
 
- **Indirect prompt injection (LLM01):** a field description authored in a source system (e.g., a Salesforce custom-field description) is attacker-controllable content that the connector would otherwise copy verbatim into catalog text later processed inside an AI assistant’s context window. Structural sanitization plus sourceOfTruth provenance tagging let AI layers treat connector-written descriptions as untrusted data. Pattern-matching for injection phrasing is deliberately not attempted (it is unreliable); the defense is structural.
 
- **Knowledge-base poisoning (LLM04):** poisoning the catalog is poisoning the grounding source for every AI feature built on it. Beyond ingestion sanitization, teams should consider metadata-drift detection (alerting when descriptions of governed assets change between runs) and Purview curation workflows before connector-written descriptions are treated as certified.
 
The full category-by-category assessment (LLM01–LLM10) is in the Purview Connector Security Review document.
 
## 9. Dependencies
 
| Package | Version | Purpose |
| --- | --- | --- |
| azure-functions | >=1.17.0 | Azure Functions Python worker (required for deployment to Azure Functions) |
| pyapacheatlas | >=0.16.0 | High-level Purview/Atlas SDK (community; see note below) |
| azure-identity | >=1.15.0 | Azure AD authentication (Managed Identity + Service Principal via DefaultAzureCredential) |
| azure-keyvault-secrets | >=4.8.0 | Retrieve data source credentials from Azure Key Vault |
| requests | >=2.31.0 | HTTP client for direct API calls (Purview REST API + data source APIs) |
| simple-salesforce | >=1.12.0 | Optional: High-level Salesforce REST API wrapper (alternative to raw requests) |
| requests-oauthlib | >=2.0.0 | OAuth 1.0a signature generation for Oracle NetSuite TBA authentication (legacy integrations only — see TBA deprecation note in Section 3.3) |
| PyJWT + cryptography | >=2.8.0 / >=42.0 | Signed JWT assertion generation for NetSuite OAuth 2.0 M2M flow (recommended for new NetSuite integrations) |
| azure-keyvault-keys | >=4.9.0 | Optional hardening: Key Vault sign operation for the NetSuite JWT so the private key never enters process memory (see Section 8.2) |
| python-dotenv | >=1.0.0 | Environment variable management (local development only) |
 
**SDK maintenance note:** pyapacheatlas (v0.16.0) is a community-maintained library whose release cadence has slowed; it remains functional because the underlying Atlas 2.2 Data Map APIs are stable, but it is no longer actively developed. Microsoft now publishes an official Python SDK for the Data Map: azure-purview-datamap (PyPI). For new development, consider the official SDK or direct REST calls via requests (as demonstrated in the connector examples, which work regardless of SDK choice). All Atlas v2 endpoint paths in this document apply identically to both approaches.
 
## 10. Example Implementations
 
Four example connector implementations are provided alongside this document, demonstrating the same core approach (custom type definitions → entity creation → lineage building → business metadata) applied to different data sources:
 
All four examples share a common set of built-in security and resilience controls (detailed in Sections 7 and 8.2): source-metadata validation and sanitization with provenance tagging, retry with exponential backoff on all live-mode HTTP calls, per-object failure isolation, a Purview endpoint allow-list, flag-gated business metadata, and injection guards on all query construction. The controls were verified against the companion Purview Connector Security Review (v1.1), which also maps the design against the OWASP GenAI Top 10 for LLM Applications.
 
**Example A: SQL Server Connector** (purview_custom_connector_example.py)
 
Demonstrates connecting to a SQL Server source and pushing metadata into Purview. Covers: authentication, custom type registration (5 entity types), entity creation with hierarchy (server → database → tables → columns), lineage building (3 source tables → process → 1 destination table), business metadata application, and classification assignment. Runs in dry-run mode by default.
 
**Example B: Salesforce Connector** (purview_salesforce_connector_example.py)
 
Demonstrates connecting to Salesforce via the REST API and pushing metadata into Purview using the exact same Atlas v2 approach. Covers: OAuth 2.0 client credentials authentication to Salesforce, dynamic discovery of Salesforce objects and fields via the Describe Global and sObject Describe APIs, custom type registration (4 entity types: salesforce_org, salesforce_object, salesforce_field, salesforce_process), entity creation with hierarchy (org → objects → fields), cross-system lineage (Salesforce objects → ETL process → data warehouse tables), business metadata application, and classification assignment. Runs in dry-run mode by default.
 
**Example C: Workday Connector** (purview_workday_connector_example.py)
 
Demonstrates connecting to Workday HCM via the REST API and pushing metadata into Purview. Covers: OAuth 2.0 refresh token authentication to Workday, metadata discovery across 10 Workday business objects (Workers, Organizations, Supervisory Organizations, Positions, Job Profiles, Compensation Plans, Cost Centers, Locations, Companies, Pay Groups), custom type registration (4 entity types: workday_tenant, workday_object, workday_field, workday_process), entity creation with hierarchy (tenant → business objects → fields), cross-system lineage including both ETL to data warehouse and real-time provisioning to Active Directory, PII field classification (email, phone, names), and business metadata application. Runs in dry-run mode by default.
 
**Example D: Oracle NetSuite Connector** (purview_netsuite_connector_example.py)
 
Demonstrates connecting to Oracle NetSuite ERP via the SuiteTalk REST API and pushing metadata into Purview. Covers: OAuth 1.0a Token-Based Authentication (TBA) to NetSuite, metadata discovery via the SuiteTalk Metadata Catalog (OpenAPI 3.0-based) and SuiteQL record counts, custom type registration (4 entity types: netsuite_account, netsuite_record_type, netsuite_field, netsuite_process), entity creation across 10 NetSuite record types (Customer, Vendor, Employee, Sales Order, Invoice, Purchase Order, Vendor Bill, Inventory Item, Journal Entry, Account), cross-system lineage covering revenue, accounts payable, inventory, and general ledger data flows, sensitive field classification (PII, financial, tax ID), and business metadata application. Runs in dry-run mode by default. **Note:** the example authenticates via TBA, which Oracle is deprecating for new integrations as of NetSuite 2027.1 — for new production deployments, replace the NetSuiteAuthService with the OAuth 2.0 Client Credentials (M2M) flow described in Section 3.3; all other services (discovery, entity, lineage, metadata) are unaffected by the authentication method.
 
## Appendix A: Key API Endpoints
 
| Operation | Method | Endpoint |
| --- | --- | --- |
| Create/Update Types | POST | /api/atlas/v2/types/typedefs |
| Get Type by Name | GET | /api/atlas/v2/types/typedef/name/{name} |
| Create/Update Entities (Bulk) | POST | /api/atlas/v2/entity/bulk |
| Get Entity by GUID | GET | /api/atlas/v2/entity/guid/{guid} |
| Search Entities | POST | /api/atlas/v2/search/advanced |
| Set Business Metadata | POST | /api/atlas/v2/entity/guid/{guid}/businessmetadata |
| Add Classifications | POST | /api/atlas/v2/entity/guid/{guid}/classifications |
| Set Labels | POST | /api/atlas/v2/entity/guid/{guid}/labels |
| Delete Entity | DELETE | /api/atlas/v2/entity/guid/{guid} |
 
All endpoints are prefixed with one of the following, depending on your account type:
 
- **Classic Purview accounts (Azure portal-based):** https://{account}.purview.azure.com/datamap
 
- **New unified Purview portal (purview.microsoft.com) / upgraded accounts:** https://api.purview-service.microsoft.com/datamap (for private endpoint setups, Microsoft recommends the tenant-specific form https://{tenant-id}-api.purview-service.microsoft.com/datamap)
 
**⚠️ ****Endpoint transition:** Microsoft is transitioning from the classic governance portal (web.purview.azure.com) to the new unified Microsoft Purview portal (purview.microsoft.com). Upgraded accounts remain accessible through **both** the legacy {account}.purview.azure.com endpoint and the new api.purview-service.microsoft.com endpoint, and the Data Map / Atlas v2 API paths are unchanged. However, Microsoft has stated the legacy endpoints will eventually be retired — plan to add the new endpoints to corporate firewalls and migrate connector configuration. The connector examples in this solution use a configurable endpoint base URL, so switching requires only a configuration change.
 
## Appendix B: References
 
- Microsoft Purview Atlas 2.2 API Tutorial: https://learn.microsoft.com/en-us/purview/data-gov-api-atlas-2-2
 
- Custom Lineage API Guide: https://learn.microsoft.com/en-us/purview/legacy/how-to-purview-custom-lineage-api-user-guide
 
- Purview API Authentication Tutorial: https://learn.microsoft.com/en-us/purview/data-gov-api-rest-data-plane
 
- Purview Service Principal Setup: https://learn.microsoft.com/en-us/purview/data-map-service-principal
 
- Purview Supported Data Sources: https://learn.microsoft.com/en-us/purview/microsoft-purview-connector-overview
 
- Purview Salesforce Connector: https://learn.microsoft.com/en-us/purview/register-scan-salesforce
 
- Salesforce REST API Developer Guide: https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_rest.htm
 
- Salesforce OAuth 2.0 Client Credentials Flow: https://developer.salesforce.com/blogs/2023/03/using-the-client-credentials-flow-for-easier-api-authentication
 
- Salesforce Integration User Best Practices: https://developer.salesforce.com/blogs/2024/02/invoke-rest-apis-with-the-salesforce-integration-user-and-oauth-client-credentials
 
- Salesforce Describe Global API: https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_describeGlobal.htm
 
- Salesforce sObject Describe API: https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_describe.htm
 
- Salesforce Connected App to External Client App Migration: https://help.salesforce.com/s/articleView?id=xcloud.connected_app_to_external_client_app_migration.htm
 
- Salesforce External Client Apps Developer Guide: https://developer.salesforce.com/docs/platform/external-client-apps/overview
 
- Workday REST API Documentation: https://doc.workday.com/admin-guide/en-us/integration-and-studio/rest-api/index.html
 
- Workday Register API Client for Integrations: https://doc.workday.com/reader/J1YlsdIIpQOqaYFWCpkPxQ/BEAMhoAqea3mBEXCd_mGdA
 
- Oracle NetSuite SuiteTalk REST Web Services: https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/chapter_1540391670.html
 
- Oracle NetSuite REST API Browser: https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/section_157373386674.html
 
- Oracle NetSuite Token-Based Authentication: https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/section_4247226122.html
 
- Oracle NetSuite — Preparing for TBA End of Support: https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/section_0525020842.html
 
- Oracle NetSuite OAuth 2.0 Client Credentials Flow: https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/section_162730264820.html
 
- Microsoft Purview — New Portal FAQ (endpoint transition): https://learn.microsoft.com/en-us/purview/data-governance-purview-portal-faq
 
- Azure Purview DataMap Python SDK (official): https://pypi.org/project/azure-purview-datamap/
 
- Purview Credentials and Key Vault: https://learn.microsoft.com/en-us/purview/data-map-data-scan-credentials
 
- pyapacheatlas SDK: https://github.com/wjohnson/pyapacheatlas
 
- OWASP GenAI Security Project — Top 10 for LLM Applications (2025): https://genai.owasp.org/llm-top-10/
 
- Companion document: Purview Connector Security Review (v1.1) — full code review findings, remediation status, and OWASP LLM01–LLM10 assessment
 
- Purview Custom Connector Solution Accelerator: https://github.com/microsoft/purview-custom-connector-solution-accelerator
 
- Purview Custom Types Tool: https://github.com/microsoft/Purview-Custom-Types-Tool-Solution-Accelerator
 
- Purview REST API Reference: https://learn.microsoft.com/en-us/rest/api/purview/
 
- Azure Functions Overview: https://learn.microsoft.com/en-us/azure/azure-functions/functions-overview
 
- Azure Key Vault Secrets SDK (Python): https://learn.microsoft.com/en-us/python/api/overview/azure/keyvault-secrets-readme
 
- DefaultAzureCredential Documentation: https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential
