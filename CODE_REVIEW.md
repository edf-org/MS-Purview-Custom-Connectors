# Code Review — MS Purview Custom Connectors

**Date:** 2026-04-01
**Reviewer:** Claude Code (claude-sonnet-4-6)
**Scope:** Full functionality and security review of all Python source files and JSON configuration.

---

## Summary

This is a well-structured, clearly documented metadata connector suite for Microsoft Purview. The overall design is sound — configuration-driven classification, Azure Key Vault credential management, dry-run safety mode, and consistent patterns across connectors are all strong choices. However, there is one critical runtime bug that will crash the SQL connector in production, several medium-severity security gaps, and a number of design consistency issues worth addressing before go-live.

---

## Files Reviewed

| File | Lines | Purpose |
|------|-------|---------|
| `classification_engine.py` | 103 | Shared rules-based field classifier |
| `classification_rules.json` | ~260 | 52 classification rules across 3 layers |
| `purview_sql_custom_connector_example.py` | ~800 | SQL Server → Purview connector |
| `purview_salesforce_connector_example.py` | ~1,400 | Salesforce → Purview connector |
| `purview_workday_connector_example.py` | ~625 | Workday → Purview connector |
| `purview_netsuite_connector_example.py` | ~905 | Oracle NetSuite → Purview connector |

---

## Critical Bug

### 1. Wrong keyword argument names in SQL connector — will raise `TypeError` at runtime

**File:** `purview_sql_custom_connector_example.py` ~line 672
**Severity:** Critical

The SQL connector calls `classify_fields()` with parameter names that do not exist in the method signature:

```python
# Called with:
col_classifications = classification_engine.classify_fields(
    source="sql",
    object_name="Orders",
    fields=columns,
    field_name_key="name",   # ← does not exist
    field_type_key="type",   # ← does not exist
)

# Actual signature in classification_engine.py:
def classify_fields(self, source, object_name, fields, name_key="name", type_key="type"):
```

Python will raise `TypeError: classify_fields() got an unexpected keyword argument 'field_name_key'` the moment this code runs. The correct parameter names are `name_key` and `type_key`. This is the only connector with this bug — the Salesforce, Workday, and NetSuite connectors call the method correctly (or use the defaults).

**Fix:** Change `field_name_key` → `name_key` and `field_type_key` → `type_key` in the SQL connector call.

---

## Security Findings

### 2. No input validation in SQL and Workday connectors

**Files:** `purview_sql_custom_connector_example.py`, `purview_workday_connector_example.py`
**Severity:** Medium

The Salesforce and NetSuite connectors define and use a `_validate_identifier()` function that blocks dangerous characters via regex and supports an allow-list. The SQL and Workday connectors have no equivalent. If field names or object names from external sources are used in API paths or query strings without validation, they become an injection vector.

**Fix:** Add `_validate_identifier()` to the SQL and Workday connectors (it can be copied directly from the Salesforce connector) and call it on any externally-sourced string before it is interpolated into API paths or Purview qualifiedNames.

### 3. `_validate_identifier()` is defined but not consistently called

**Files:** `purview_salesforce_connector_example.py`, `purview_netsuite_connector_example.py`
**Severity:** Medium

Even in the connectors that define `_validate_identifier()`, there is no evidence it is called on field names arriving from the source API responses before those names are used to build Purview qualifiedNames or API paths. The function exists but appears unused outside of the comments explaining it. If a source system returns a field with a name like `../evil` or `'; DROP TABLE`, it would flow unchecked into the qualified name builder.

**Fix:** Call `_validate_identifier()` on every `field["name"]` and object name retrieved from the source API response before using them in `_qualified_name()` or any API call.

### 4. No retry or backoff — creates a silent failure mode, not a security issue per se, but relevant to token handling

**Files:** All connectors
**Severity:** Low

All HTTP calls are single-attempt with `raise_for_status()`. A 429 (rate limited) or transient 5xx response from Purview or the source API will cause the entire connector run to abort with an unhandled exception. More critically for security: there is no token expiry handling. If a bearer token expires mid-run (Purview tokens typically expire after 1 hour), the connector will fail with a 401 and expose the token in the exception log depending on logging configuration.

**Fix:** Add exponential backoff with a max retry count for transient errors. Re-acquire tokens on 401 responses rather than propagating the error.

### 5. `import re` inside function body — minor but inconsistent

**Files:** `purview_salesforce_connector_example.py`, `purview_netsuite_connector_example.py`, `purview_workday_connector_example.py`
**Severity:** Low (style/practice)

`_validate_identifier()` in each connector imports `re` inside the function body on every call. This is a minor performance inefficiency and deviates from the standard practice of top-level imports, which also makes security auditing easier (you can see all dependencies at a glance).

**Fix:** Move `import re` to the top-level imports of each file.

### 6. `PurviewConfig` dataclass is duplicated across three files

**Files:** `purview_salesforce_connector_example.py`, `purview_netsuite_connector_example.py`, `purview_workday_connector_example.py`
**Severity:** Low (maintainability / security hygiene)

`PurviewConfig` — including its `from_environment()` and `endpoint` property — is copy-pasted verbatim into three files. The SQL connector has its own version too. Any security patch to this class (e.g., adding input sanitization to `account_name` before it is interpolated into the endpoint URL) would need to be applied four times. An attacker who can influence the `PURVIEW_ACCOUNT_NAME` environment variable could potentially redirect API calls to a different host.

**Fix:** Extract `PurviewConfig` into a shared `purview_common.py` module imported by all connectors. Apply sanitization to `account_name` (strip whitespace, validate it matches `^[a-zA-Z0-9-]+$`).

### 7. Endpoint URL built by direct string interpolation of `account_name`

**Files:** All connectors
**Severity:** Low

```python
@property
def endpoint(self) -> str:
    return f"https://{self.account_name}.purview.azure.com/datamap"
```

If `account_name` contains `@` or `.` or path separators, the constructed URL would be malformed or could point to an unintended host. While this value comes from an environment variable (controlled deployment context), validating it at construction time is cheap and adds defense-in-depth.

**Fix:** Add a check that `account_name` matches `^[a-zA-Z0-9-]+$` before building the URL.

### 8. Secret values logged in dry-run mode

**Files:** `purview_salesforce_connector_example.py`, `purview_netsuite_connector_example.py`, `purview_workday_connector_example.py`
**Severity:** Low

Dry-run placeholder values like `"dry-run-consumer-key"`, `"dry-run-sf-token"`, and `"dry-run-purview-token"` are stored in config objects and could be printed if the config object is ever logged directly (e.g., `logger.debug(config)`). While they are not real secrets, the pattern of storing credential-shaped strings in the same fields that will hold real secrets in production creates a risk that real secrets could accidentally be logged in a future change.

**Fix:** Mark credential fields with `field(repr=False)` in the dataclass definitions so they are excluded from default string representations.

---

## Functionality Findings

### 9. Empty-string key stored in results when `name_key` is missing from a field dict

**File:** `classification_engine.py` line 61
**Severity:** Medium

```python
def classify_fields(self, source, object_name, fields, name_key="name", type_key="type"):
    results = {}
    for f in fields:
        c = self.classify_field(source, object_name, f.get(name_key, ""), f.get(type_key))
        if c: results[f.get(name_key, "")] = c   # ← stores "" as key if name_key absent
```

If any field dict is missing the key named by `name_key`, `f.get(name_key, "")` returns `""` and the classification is stored under an empty-string key. Subsequent lookups for a specific field by name would miss the result, and the empty-string key would be silently written over on each iteration if multiple fields lack the name key.

**Fix:** Skip fields where the name key is absent or empty: `if not f.get(name_key): continue`.

### 10. `Optional` imported but never used in `classification_engine.py`

**File:** `classification_engine.py` line 19
**Severity:** Low (dead code)

```python
from typing import Optional
```

`Optional` is imported but not referenced anywhere in the file (the function `classify_field` returns `Optional[str]` implicitly but the annotation is not written). Dead imports clutter the dependency surface.

**Fix:** Remove the import or add the return type annotation `-> Optional[str]` to `classify_field`.

### 11. Production mode activation requires manual code edits — error-prone

**Files:** All connectors
**Severity:** Medium (operational risk)

Transitioning from dry-run to live mode requires a developer to manually uncomment dozens of code blocks scattered across each file and delete the corresponding dry-run blocks. This is fragile: a missed uncomment silently continues to dry-run (data is not pushed), while an accidental partial edit could produce half-live behavior. There is no test that confirms all real code paths are active.

**Fix:** Replace the comment-based activation pattern with an environment-variable flag (e.g., `DRY_RUN=true/false`). Gate dry-run behavior with `if DRY_RUN:` conditionals rather than requiring code changes. This also makes it testable.

### 12. No incremental / change-data-capture scan

**Files:** All connectors
**Severity:** Medium (scalability)

Every connector run re-discovers and re-pushes all entities from scratch. For Salesforce orgs with hundreds of objects or SQL databases with thousands of columns, this creates unnecessary load on both the source system and Purview, and obscures whether anything actually changed between runs.

**Fix:** Track a `last_scanned` timestamp (in a persistent store or Purview business metadata attribute). On subsequent runs, only push entities whose schema has changed since the last scan.

### 13. No orphan cleanup

**Files:** All connectors
**Severity:** Low

If a Salesforce object, Workday business object, or SQL table is deleted from the source system, its corresponding Purview entity will remain indefinitely. Over time the data map will accumulate stale entities, which undermines trust in the catalog.

**Fix:** After a full scan, compare the set of qualifiedNames pushed in the current run against the set that exists in Purview. Soft-delete (set status to `DELETED`) any entities no longer present in the source.

### 14. Batch size is hardcoded with no configuration option

**Files:** `purview_sql_custom_connector_example.py` (class constant `BATCH_SIZE = 50`)
**Severity:** Low

The batch size of 50 is a class-level constant. Different environments (high-throughput Purview instances vs. shared tenants under rate limiting) may need different batch sizes. It should be externally configurable.

**Fix:** Read `BATCH_SIZE` from an environment variable with a sensible default.

### 15. No unit tests

**Files:** All
**Severity:** Medium

The only test coverage is the `classification_engine.py` self-test (run as `__main__`). There are no unit tests for the connector logic, entity builders, lineage service, or qualifiedName construction. The critical bug in finding #1 would have been caught by a trivial unit test.

**Fix:** Add a `tests/` directory with at minimum: unit tests for `classify_field` edge cases, a test that instantiates each connector in dry-run mode and asserts on the returned entity list, and a test that calls `classify_fields` with the same kwargs used in each connector to validate the parameter names are correct.

### 16. `logging.basicConfig` called at module import time in every connector

**Files:** All connectors
**Severity:** Low

```python
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
```

This is called at module level in every connector file. `basicConfig` is a no-op if any handler is already configured (including by Azure Functions' host). If multiple connectors are imported into the same process (e.g., an Azure Function App importing all of them), only the first call takes effect. Callers lose the ability to configure their own logging format.

**Fix:** Remove `basicConfig` from the connector modules. Let the caller (e.g., `function_app.py` or `__main__` block) configure logging.

---

## Classification Rules Review (`classification_rules.json`)

### 17. `*descriptor*` pattern is overly broad

**File:** `classification_rules.json`
**Severity:** Medium

```json
{ "pattern": "*descriptor*", "classification": "MICROSOFT.PERSONAL.NAME", "priority": 5 }
```

The pattern `*descriptor*` matches any field containing the word "descriptor" — including non-PII fields like `productDescriptor`, `errorDescriptor`, `typeDescriptor`, etc. This will produce false-positive PII classifications on common technical field names. The priority of 5 means it can be overridden by exact rules, but if no exact rule exists for a field like `productDescriptor`, it will be incorrectly tagged as a personal name.

**Fix:** Raise the specificity: use `"*workdescriptor*"` or `"*worker_descriptor*"`, or remove the generic pattern and rely solely on the exact `object_field_rules` entries for Workday's `descriptor` field.

### 18. No rule covers `*name*` for general name fields

**File:** `classification_rules.json`
**Severity:** Low (coverage gap)

Fields like `customerName`, `contactName`, or `vendorName` would not be classified as `MICROSOFT.PERSONAL.NAME` because the name patterns only cover `*firstname*`, `*last_name*`, `*fullname*`, etc. A field simply called `name` on a contact or customer object would fall through unclassified unless covered by an exact object_field_rule.

**Consideration:** A `*name*` pattern would be very broad (matching `filename`, `typename`, `companyName`, etc.), so this is a deliberate tradeoff. The gap is worth documenting explicitly in the JSON metadata.

### 19. Priority conflict: `*descriptor*` (priority 5) vs field_type_rules (priority 3–5)

**File:** `classification_rules.json`
**Severity:** Low

The `*descriptor*` name pattern has `priority: 5`, the same as the `email` and `phone` type rules (also priority 5). If a field named `emailDescriptor` has type `email`, both rules match at the same priority. The winner is determined by sort order in the match list, which is insertion order (exact rules first, then name patterns, then type rules). This means the name pattern (`MICROSOFT.PERSONAL.NAME`) would win over the type rule (`MICROSOFT.PERSONAL.EMAIL`) for a field named `emailDescriptor`, which is probably not the intended behavior.

**Fix:** Lower `*descriptor*` to priority 4 so type-based rules win in ambiguous cases.

---

## Positive Observations

These are worth preserving and not changing:

- **Dry-run by default**: All connectors are safe to run without credentials, which is excellent for onboarding and validation.
- **Azure Key Vault integration**: Credentials are never hardcoded; the Key Vault pattern is correctly used across all connectors.
- **`REQUEST_TIMEOUT = (10, 30)`**: Explicit connect and read timeouts on every file prevent indefinite hangs.
- **Batch entity creation**: The 50-entity batch cap prevents memory overflow on large schemas.
- **Three-tier priority system in classification rules**: The `object_field_rules` → `field_name_patterns` → `field_type_rules` priority hierarchy is clean and predictable.
- **`"enabled": false` flag in rules**: Allows rules to be disabled without deletion, preserving history.
- **Consistent connector structure**: All four connectors follow the same architectural pattern (config → auth → type defs → entities → lineage → metadata), making the codebase easy to extend.
- **Purview `qualifiedName` scheme**: The `source://server/db/schema/table/column` URI pattern is consistent across connectors and will produce correct deduplication on upsert.

---

## Findings Summary

| # | Finding | Severity | File(s) |
|---|---------|----------|---------|
| 1 | Wrong kwargs in `classify_fields` call — `TypeError` at runtime | **Critical** | `purview_sql_custom_connector_example.py` |
| 2 | No `_validate_identifier` in SQL and Workday connectors | Medium | SQL, Workday connectors |
| 3 | `_validate_identifier` defined but not called on API response data | Medium | Salesforce, NetSuite connectors |
| 4 | No retry/backoff; 401 token expiry not handled | Low | All connectors |
| 5 | `import re` inside function body | Low | Salesforce, NetSuite, Workday connectors |
| 6 | `PurviewConfig` duplicated across four files | Low | All connectors |
| 7 | `account_name` interpolated into URL without validation | Low | All connectors |
| 8 | Credential fields not hidden from `repr` | Low | All connectors |
| 9 | Empty-string key written to results in `classify_fields` | Medium | `classification_engine.py` |
| 10 | `Optional` imported but unused | Low | `classification_engine.py` |
| 11 | Production mode requires manual code edits | Medium | All connectors |
| 12 | No incremental/CDC scan | Medium | All connectors |
| 13 | No orphan entity cleanup | Low | All connectors |
| 14 | `BATCH_SIZE` not configurable | Low | SQL connector |
| 15 | No unit tests | Medium | All files |
| 16 | `logging.basicConfig` at module import time | Low | All connectors |
| 17 | `*descriptor*` pattern too broad — false-positive PII | Medium | `classification_rules.json` |
| 18 | No coverage for generic `*name*` fields | Low | `classification_rules.json` |
| 19 | Priority tie between `*descriptor*` and type rules | Low | `classification_rules.json` |

---

*This review covers the example connector implementations as written. Findings marked Critical and Medium should be addressed before using these connectors with real Purview and source system credentials.*
