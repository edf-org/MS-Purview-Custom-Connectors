# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Classification Engine

```bash
# Run the built-in self-test (validates all rules load and match correctly)
python classification_engine.py

# Install dependencies
pip install pyapacheatlas azure-identity azure-keyvault-secrets requests python-dotenv
```

Requirements: Python 3.9+, `pyapacheatlas` >= 0.14.0.

## Architecture

This repo implements custom connectors that push metadata from SaaS/database sources into Microsoft Purview Data Map where native scanning has classification limitations.

**Core pattern:** Each connector (Salesforce, NetSuite, Workday, SQL Server) follows the same structure:
1. Authenticate to the source system (OAuth 2.0 / OAuth 1.0a / Service Principal)
2. Discover metadata (objects/fields) via the source API
3. Classify fields using the shared `ClassificationEngine`
4. Register type definitions and create entity hierarchies in Purview via Atlas v2 API (`pyapacheatlas`)
5. Build cross-system lineage and apply business metadata

**Classification Engine** (`classification_engine.py` + `classification_rules.json`):
- Three rule layers evaluated per field; highest priority wins:
  - `object_field_rules` — exact source + object + field match (priority 50)
  - `field_name_patterns` — wildcard on field name (priority 10 default)
  - `field_type_rules` — match on source API data type (priority 5)
- Rules file is maintained by data stewards (no Python required). Add a rule entry to `classification_rules.json` and re-run the connector — no code changes needed.
- Disable a rule without deleting it by setting `"enabled": false`.
- This is **rule-based** (field names/types/context), not content-based classification. For actual data-value inspection in SaaS sources, complement with Salesforce Shield Data Detect or Microsoft Defender for Cloud Apps.

**Credentials:** Stored in Azure Key Vault; connectors use Managed Identity or Service Principal auth. Never hardcode credentials.

## Key Files

| File | Purpose |
|------|---------|
| `classification_engine.py` | Shared module imported by all connectors |
| `classification_rules.json` | Rules maintained by data stewards |
| `purview_salesforce_connector_example.py` | Salesforce → Purview (OAuth 2.0) |
| `purview_netsuite_connector_example.py` | NetSuite → Purview (OAuth 1.0a) |
| `purview_workday_connector_example.py` | Workday → Purview (OAuth 2.0 + refresh) |
| `purview_sql_custom_connector_example.py` | SQL Server → Purview (Service Principal) |
| `Purview_Custom_API_Architecture.docx` | Full architecture & implementation guide |

## Classification Labels

Classifications use the `MICROSOFT.*` namespace (e.g., `MICROSOFT.PERSONAL.EMAIL`). Full list: https://learn.microsoft.com/en-us/purview/supported-classifications
