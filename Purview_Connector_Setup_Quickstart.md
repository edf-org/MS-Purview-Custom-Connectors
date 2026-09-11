

# Purview Custom Connector — High-Level Setup Guide
 
A condensed checklist of the steps needed to stand up the custom API solution. For full detail, see the Architecture Document (section references noted throughout).
 
---
 
## Phase 1: Azure Foundation
 
1. **Provision a Microsoft Purview account** (Data Map enabled) in your Azure subscription, if not already in place.
2. **Create an Azure Key Vault** to hold all data source credentials (Salesforce, Workday, NetSuite, etc.). Never store these in code or config files.
3. **Purview authentication method: Managed Identity** (Architecture Doc, Section 3). The connector will run in Azure and authenticate to both Purview and Key Vault using the Function App's System-Assigned Managed Identity — no client secrets to create, store, or rotate. Note: Managed Identity only works when running inside Azure, so local testing is done in dry-run mode (no credentials required) and live validation happens after deploying to the Function App.
## Phase 2: Grant Purview Access
 
4. In the **Purview governance portal** → Data Map → Collections, create or select a dedicated collection for the source (recommended: create a collection per source system, e.g., "Salesforce", "Workday", etc., rather than using the root collection). Then, in **Role assignments** for that collection, assign the Function App's managed identity (this happens after the Function App is created in Phase 5 — search for it by the Function App name):
   - **Data Curator** — entity creation, lineage, classifications (scoped to the collection)
   - **Data Source Administrator** — registering sources, managing scans (scoped to the collection)
5. Grant the same identity **Key Vault Secrets User** on your Key Vault so the connector can read source credentials.
> Note: Only a Collection Admin can assign these roles — loop in your Purview administrator if you don't see the option.
 
## Phase 3: Configure the Data Source Side
 
6. Set up authentication on each source system you're connecting (Architecture Doc, Section 3.3):
   - **Salesforce** — Connected App with OAuth 2.0 Client Credentials Flow, "api" scope, and a dedicated Run As integration user. Capture the Consumer Key/Secret.
   - **Workday** — API client with OAuth 2.0 refresh token grant.
   - **NetSuite** — Enable REST Web Services + Token-Based Authentication, create an Integration Record and Access Token. Capture all four values (consumer key/secret, token ID/secret) — they're shown only once.
7. **Store all source credentials as secrets in Key Vault** using the naming conventions in each connector's header comments.
## Phase 4: Local Development & Validation
 
8. Install **Python 3.11** locally (matching the Function App runtime) and the required packages:
   ```
   pip install pyapacheatlas azure-identity azure-keyvault-secrets requests requests-oauthlib python-dotenv
   ```
9. Create a **`.env` file** (add it to `.gitignore`) with `PURVIEW_ACCOUNT_NAME` and `KEY_VAULT_URL`. With Managed Identity, no tenant/client ID or client secret variables are needed.
10. **Run a connector in dry-run mode** (the default — no credentials needed) to validate the logic:
    ```
    python purview_salesforce_connector_example.py
    ```
11. Since Managed Identity is only available inside Azure, **live-mode validation happens after deployment** (Phase 5): uncomment the "real usage" blocks, deploy, and run against a test collection to confirm entities, lineage, and classifications land correctly in the Data Map.
## Phase 5: Production Deployment (Azure Functions)
 
12. **Create a Function App** with the **Flex Consumption** hosting plan, using these settings:
    - **Resource Group**: create a new dedicated group, e.g. `rg-purview-connector-prod`
    - **Function App name**: globally unique, e.g. `func-purview-connector-prod` (prefix your company abbreviation if taken)
    - **Runtime stack**: Python, **version 3.11** (avoid the newest releases — pyapacheatlas and related dependencies are validated on 3.11; match this version locally)
    - **Region**: same region as your Purview account
    - **Instance size**: keep the default 2048 MB — metadata scans are lightweight, and Flex only bills memory during execution
    - **Zone redundancy**: off — a missed nightly scan is simply picked up by the next run, so zone-level resilience adds cost/complexity with no benefit
    - **Storage** tab: create or select a storage account (required by Functions; Flex also stores your deployment package here — don't delete or restrict it later)
    - **Monitoring** tab: enable Application Insights
    (Architecture Doc, Section 6)
13. **Enable System-Assigned Managed Identity** on the Function App, then assign it the Purview roles (step 4) and Key Vault role (step 5).
14. **Add application settings**: `PURVIEW_ACCOUNT_NAME` and `KEY_VAULT_URL` only — Managed Identity means no `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, or `AZURE_CLIENT_SECRET` settings anywhere.
15. **Deploy the code** via Azure Functions Core Tools (`func azure functionapp publish <app-name>`), VS Code, or a CI/CD pipeline — make sure Core Tools is up to date (`npm install -g azure-functions-core-tools@4`) since older builds predate Flex Consumption support. Structure the project with `function_app.py` (Timer + HTTP triggers), `host.json`, and `requirements.txt` wrapping the connector library.
16. **Verify**: run the Timer trigger manually via Code + Test, hit the HTTP trigger with curl/Postman, and confirm successful runs in Application Insights.
## Phase 6: Customize & Operate
 
17. Edit each connector's scan scope (e.g., `RECORD_TYPES_TO_SCAN`) and `LINEAGE_MAPPINGS` to match your environment.
18. Set the Timer trigger schedule for recurring scans; use the HTTP trigger for on-demand runs.
19. Monitor via Application Insights; the connectors handle rate limiting, retries, and duplicate prevention (upsert on `qualifiedName`) automatically.
## Phase 7: Rollback & Recovery
 
The connectors include rollback support: every scan run generates a unique **scan run ID** (logged at startup, visible in Application Insights), and every entity written is stamped with it via a searchable `scanRunId` attribute.
 
20. **Tag every production release** in git (or keep versioned artifacts in CI/CD). Code rollback on Flex Consumption has no deployment-slot swap — it's simply redeploying the last known-good package: `git checkout <last-good-tag>` then `func azure functionapp publish <app-name>`.
21. **Know the kill switch**: disabling the Timer trigger (or stopping the Function App) halts all scanning immediately with zero impact on existing Purview metadata.
22. **Pick the right recovery path** for a bad run:
    - **Wrong values written to existing entities** → don't roll back. Fix the connector, redeploy, re-run — the upsert on `qualifiedName` overwrites the bad values (self-healing).
    - **Entities created that shouldn't exist** → use `purview_rollback_scan_run.py`. Grab the run ID from the logs, dry-run it first, then execute:
      ```
      python purview_rollback_scan_run.py --run-id <bad-run-id>            # lists what would be deleted
      python purview_rollback_scan_run.py --run-id <bad-run-id> --execute  # soft-deletes it
      ```
      Deletion is a soft delete — legitimate assets are recreated fresh by the next healthy scan. Run this manually from your workstation (`az login` with Data Curator rights); never deploy it to the Function App.
23. **Record the run ID before and after every production scan** — it's the handle everything above depends on.
---
 
**Minimum viable path:** Purview account → Key Vault → source credentials in Key Vault → dry-run locally → create Function App + enable Managed Identity → assign Purview and Key Vault roles to the identity → deploy → live test against a test collection.
 
