

# Microsoft Purview – Data Management Maturity Plan
 
## Project Repository
 
This repository contains the Microsoft Purview Data Management Maturity Roadmap, the custom API architecture guide, the connector security review and its CI pipelines, custom connector implementations, and the configuration-driven classification engine.
 
---
 
### 📂 Repository Structure
 
```
├── Microsoft_Purview_Data_Management_Maturity_Plan.md      # Roadmap — Markdown version (incl. DAMA, Batchelder, connector strategy)
├── Purview_Custom_API_Architecture.md                      # Architecture & Implementation Guide
├── Purview_Connector_Security_Review.md                    # Security review of the connector codebase (L1–L4 findings)
├── ci_dependency_lock_github.yml                           # L4 remediation — dependency lock + SCA (GitHub Actions)
├── ci_dependency_lock_azure.yml                            # L4 remediation — dependency lock + SCA (Azure DevOps)
├── README.md                                                # This file
├── Purview_Connector_Setup_Quickstart.md                    # High-level setup quickstart (Phases 1-7, incl. rollback)
├── purview_rollback_scan_run.py                             # Scan-run rollback utility (dry-run by default)
├── classification_engine.py                                 # Shared classification engine (all connectors import this)
├── classification_rules.json                                # Classification rules (data stewards maintain this)
├── purview_salesforce_connector_example.py                  # Salesforce → Purview connector
├── purview_netsuite_connector_example.py                    # Oracle NetSuite → Purview connector
├── purview_workday_connector_example.py                     # Workday → Purview connector
└── purview_sql_custom_connector_example.py                  # SQL Server → Purview connector
```
 
**Related deliverables maintained outside this repository:**
 
- `Microsoft_Purview_Data_Management_Maturity_Plan.pptx` — the 36-slide master presentation (Executive Summary through DAMA DMBOK2 alignment, Batchelder best practices, and Custom Connector Strategy). The Markdown version above carries the same content in document form.
- `Data_Stewards_Briefing.pptx` — 8-slide briefing on the Data Steward role and the federated stewardship model.
---
 
### 📐 Architecture Document
 
**Purview_Custom_API_Architecture.md** covers:
 
- Solution architecture overview (orchestration, connectors, core services, Purview Data Map)
- Authentication strategy (Managed Identity, Service Principal, dual-auth patterns)
- Core capabilities (type definitions, entity creation, lineage, business metadata)
- **Section 4.4.1: Configuration-Driven Classification Engine** — documents the engine approach, priority system, integration code, and limitations
- Project structure
- Deployment recommendations (Azure Functions, ADF, Container Apps)
- Error handling, security considerations, and API endpoint reference
---
 
### 🔒 Security Review & CI Pipelines
 
**Purview_Connector_Security_Review.md** documents the security assessment of the connector codebase, including supply-chain (L4) and least-privilege / LLM06 hardening recommendations.
 
- **All four connectors** apply the review's recommendations in code: source-metadata sanitization for qualifiedNames and descriptions (M1), HTTP timeouts with retry/backoff (M2), business-metadata gating behind `APPLY_BUSINESS_METADATA` (M3), a validated `PURVIEW_ENDPOINT` allow-list (L1), discovery breadth caps via `MAX_OBJECTS_PER_RUN` / `MAX_FIELDS_PER_OBJECT` (LLM10), metadata drift detection via `DRIFT_STATE_PATH` (LLM04), and least-privilege collection scoping guidance — one Service Principal per connector on a dedicated collection (LLM06).
- **`ci_dependency_lock_github.yml`** / **`ci_dependency_lock_azure.yml`** implement the L4 remediation as CI pipelines: generate a hash-pinned lock file from `requirements.in`, run an SCA scan (`pip-audit`), and verify a `--require-hashes` install.
---
 
### ↩️ Scan Run Rollback Support
 
Every connector run generates a unique scan run ID (logged at startup, captured in Application Insights) and stamps it on every entity it writes via a searchable `scanRunId` attribute defined on all custom types. If a bad run creates entities that should not exist, **`purview_rollback_scan_run.py`** locates everything stamped with that run ID and soft-deletes it:
 
```bash
python purview_rollback_scan_run.py --run-id <bad-run-id>            # dry run: lists what would be deleted
python purview_rollback_scan_run.py --run-id <bad-run-id> --execute  # soft-deletes (typed confirmation required)
```
 
If a bad run merely wrote wrong values onto existing entities, do **not** roll back — fix the connector and re-run; the upsert on `qualifiedName` overwrites the bad values. See **Purview_Connector_Setup_Quickstart.md** Phase 7 for the full decision tree, release tagging, and kill-switch procedure.
 
---
 
### 🔌 Custom Connectors
 
These Python scripts implement custom connectors for data sources where Purview's native scanning has limitations (e.g., no automated classification for Salesforce, NetSuite, or Workday).
 
| Connector | Source System | Auth Method | Key Features |
|-----------|--------------|-------------|--------------|
| `purview_salesforce_connector_example.py` | Salesforce CRM | OAuth 2.0 | Object/field discovery, classifications via engine, cross-system lineage |
| `purview_netsuite_connector_example.py` | Oracle NetSuite | OAuth 1.0a (TBA) | Record type/field discovery, classifications via engine, lineage to DW/BI |
| `purview_workday_connector_example.py` | Workday HCM | OAuth 2.0 + refresh | Business object discovery, classifications via engine, lineage to AD |
| `purview_sql_custom_connector_example.py` | SQL Server | Service Principal | DB → schema → table → column hierarchy, classifications via engine, lineage |
 
All four connectors use the shared **Classification Engine** instead of hardcoded classification logic, and all four carry the full Security Review hardening (see above).
 
---
 
### 🏷️ Classification Engine
 
The classification engine separates "what to classify" from "how to classify":
 
**`classification_rules.json`** — Maintained by data stewards (no Python knowledge required). Contains 53 rules across three layers:
 
| Rule Layer | Priority | How It Matches | Example |
|------------|----------|---------------|---------|
| Object-field rules | 50 (highest) | Exact match: source + object + field | `salesforce/Contact/Email` → `MICROSOFT.PERSONAL.EMAIL` |
| Field name patterns | 10 | Wildcard on field name | `*phone*` → `MICROSOFT.PERSONAL.PHONE_NUMBER` |
| Field type rules | 5 (lowest) | Match on source API data type | `currency` → `MICROSOFT.FINANCIAL.AMOUNT` |
 
When multiple rules match the same field, the highest priority wins. Rules can be disabled by setting `"enabled": false`.
 
**`classification_engine.py`** — Shared Python module imported by all connectors. Usage:
 
```python
from classification_engine import ClassificationEngine
 
engine = ClassificationEngine()
result = engine.classify_field("salesforce", "Contact", "Email", "email")
# Returns: "MICROSOFT.PERSONAL.EMAIL"
 
batch = engine.classify_fields("salesforce", "Contact", discovered_fields)
# Returns: {"Email": "MICROSOFT.PERSONAL.EMAIL", "Phone": "MICROSOFT.PERSONAL.PHONE_NUMBER", ...}
```
 
`classify_fields()` accepts either `name_key`/`type_key` or `field_name_key`/`field_type_key` keyword arguments — both spellings are supported.
 
Run the built-in self-test: `python classification_engine.py`
 
**To add a new classification rule:** Open `classification_rules.json`, add a rule entry, re-run the connector. No Python changes needed. (Example: rule 1.1 added `*amount*` → `MICROSOFT.FINANCIAL.AMOUNT` so SQL `decimal` columns like `TotalAmount` classify correctly.)
 
**Important limitation:** This is rule-based classification (field names, types, object context), not content-based classification (inspecting actual data values). For content-level detection in SaaS sources, complement with Salesforce Shield Data Detect or Microsoft Defender for Cloud Apps.
 
---
 
### ⚙️ Prerequisites
 
```bash
pip install pyapacheatlas azure-identity azure-keyvault-secrets requests python-dotenv
```
 
- Python 3.9+
- `pyapacheatlas` >= 0.14.0
- Azure subscription with Microsoft Purview Enterprise instance
**Production deployments:** install from a hash-pinned lock file (`pip install --require-hashes -r requirements.txt`) generated by the CI pipelines above — see the Security Review (L4).
 
---
 
### 📚 References
 
- **Microsoft Purview Documentation**: https://learn.microsoft.com/en-us/purview/
- **DAMA DMBOK2R**: DAMA International, 2024
- **Data Governance Handbook**: Wendy S. Batchelder, Packt Publishing, 2024
- **Purview Supported Classifications**: https://learn.microsoft.com/en-us/purview/supported-classifications
- **Microsoft Defender for Cloud Apps**: https://learn.microsoft.com/en-us/defender-cloud-apps/
---
 
*Version 1.3 | July 2026 | CONFIDENTIAL – FOR INTERNAL USE ONLY*
 
