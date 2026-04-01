# Microsoft Purview – Data Management Maturity Plan

## Project Repository

This repository contains the comprehensive Microsoft Purview Data Management Maturity Roadmap, the custom API architecture guide, custom connector implementations, and the configuration-driven classification engine.

---

### 📂 Repository Structure

```
├── Purview_Custom_API_Architecture.docx                   # Architecture & Implementation Guide
├── README.md                                               # This file
└── connectors/                                             # Custom connectors + classification engine
    ├── classification_engine.py                            # Shared engine (all connectors import this)
    ├── classification_rules.json                           # Rules file (data stewards maintain this)
    ├── purview_salesforce_connector_example.py             # Salesforce → Purview connector
    ├── purview_netsuite_connector_example.py               # Oracle NetSuite → Purview connector
    ├── purview_workday_connector_example.py                # Workday → Purview connector
    └── purview_sql_custom_connector_example.py             # SQL Server → Purview custom connector
```

---

### 📐 Architecture Document

**Purview_Custom_API_Architecture.docx** covers:

- Solution architecture overview (orchestration, connectors, core services, Purview Data Map)
- Authentication strategy (Managed Identity, Service Principal, dual-auth patterns)
- Core capabilities (type definitions, entity creation, lineage, business metadata)
- **Section 4.4.1: Configuration-Driven Classification Engine** — documents the engine approach, priority system, integration code, and limitations
- Project structure (updated to include `classification/` directory)
- Deployment recommendations (Azure Functions, ADF, Container Apps)
- Error handling, security considerations, and API endpoint reference

---

### 🔌 Custom Connectors

These Python scripts implement custom connectors for data sources where Purview's native scanning has limitations (e.g., no automated classification for Salesforce, NetSuite, or Workday).

| Connector | Source System | Auth Method | Key Features |
|-----------|--------------|-------------|--------------|
| `purview_salesforce_connector.py` | Salesforce CRM | OAuth 2.0 | Object/field discovery, classifications via engine, cross-system lineage |
| `purview_netsuite_connector.py` | Oracle NetSuite | OAuth 1.0a (TBA) | Record type/field discovery, classifications via engine, lineage to DW/BI |
| `purview_workday_connector.py` | Workday HCM | OAuth 2.0 + refresh | Business object discovery, classifications via engine, lineage to AD |
| `purview_sql_connector.py` | SQL Server | Service Principal | DB → schema → table → column hierarchy, classifications via engine, lineage |

All four connectors have been updated to use the shared **Classification Engine** instead of hardcoded classification logic.

---

### 🏷️ Classification Engine

The classification engine separates "what to classify" from "how to classify":

**`classification_rules.json`** — Maintained by data stewards (no Python knowledge required). Contains 52 rules across three layers:

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

Run the built-in self-test: `python classification_engine.py`

**To add a new classification rule:** Open `classification_rules.json`, add a rule entry, re-run the connector. No Python changes needed.

**Important limitation:** This is rule-based classification (field names, types, object context), not content-based classification (inspecting actual data values). For content-level detection in SaaS sources, complement with Salesforce Shield Data Detect or Microsoft Defender for Cloud Apps.

---

### ⚙️ Prerequisites

```bash
pip install pyapacheatlas azure-identity azure-keyvault-secrets requests python-dotenv
```

- Python 3.9+
- `pyapacheatlas` >= 0.14.0
- Azure subscription with Microsoft Purview Enterprise instance

---

### 📚 References

- **Microsoft Purview Documentation**: https://learn.microsoft.com/en-us/purview/
- **DAMA DMBOK2R**: DAMA International, 2024
- **Data Governance Handbook**: Wendy S. Batchelder, Packt Publishing, 2024
- **Purview Supported Classifications**: https://learn.microsoft.com/en-us/purview/supported-classifications

---

*Version 1.0 | March 2026 | CONFIDENTIAL – FOR INTERNAL USE ONLY*
