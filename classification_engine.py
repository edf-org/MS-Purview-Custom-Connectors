"""
Classification Engine for Microsoft Purview Custom Connectors
================================================================
Loads rules from classification_rules.json. Any connector calls:

    from classification_engine import ClassificationEngine
    engine = ClassificationEngine()
    result = engine.classify_field("salesforce", "Contact", "Email", "email")
    # -> "MICROSOFT.PERSONAL.EMAIL"

Three rule layers evaluated (highest priority wins):
  1. object_field_rules  - exact source+object+field (priority 50)
  2. field_name_patterns - wildcard on field name (priority 10)
  3. field_type_rules    - match on API data type (priority 5)

Data stewards edit classification_rules.json only. No Python changes needed.
"""
import json, fnmatch, logging, os
from typing import Optional

logger = logging.getLogger(__name__)

class ClassificationEngine:
    def __init__(self, rules_path=None):
        if rules_path is None:
            rules_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "classification_rules.json")
        if not os.path.exists(rules_path):
            raise FileNotFoundError(f"Rules not found: {rules_path}")
        with open(rules_path, "r", encoding="utf-8") as f:
            c = json.load(f)
        self._name = [r for r in c.get("field_name_patterns", []) if r.get("enabled", True)]
        self._type = [r for r in c.get("field_type_rules", []) if r.get("enabled", True)]
        self._exact = [r for r in c.get("object_field_rules", []) if r.get("enabled", True)]
        logger.info(f"Classification engine: {len(self._name)} name, {len(self._type)} type, {len(self._exact)} exact rules")

    def classify_field(self, source, object_name, field_name, field_type=None):
        matches = []
        for r in self._exact:
            if r["source"].lower()==source.lower() and r["object"].lower()==object_name.lower() and r["field"].lower()==field_name.lower():
                matches.append((r["priority"], r["classification"], "exact"))
        fl = field_name.lower()
        for r in self._name:
            if fnmatch.fnmatch(fl, r["pattern"].lower()):
                matches.append((r["priority"], r["classification"], "name"))
        if field_type:
            tl = field_type.lower()
            for r in self._type:
                if r["type"].lower() == tl:
                    matches.append((r["priority"], r["classification"], "type"))
        if not matches:
            return None
        matches.sort(key=lambda x: x[0], reverse=True)
        w = matches[0]
        logger.debug(f"  {source}/{object_name}/{field_name} -> {w[1]} (pri={w[0]}, by={w[2]})")
        return w[1]

    def classify_fields(self, source, object_name, fields, name_key="name", type_key="type"):
        results = {}
        for f in fields:
            c = self.classify_field(source, object_name, f.get(name_key,""), f.get(type_key))
            if c: results[f.get(name_key,"")] = c
        logger.info(f"Classified {len(results)}/{len(fields)} fields on {source}/{object_name}")
        return results

    def get_stats(self):
        return {"name_patterns": len(self._name), "type_rules": len(self._type), "object_field_rules": len(self._exact),
                "total": len(self._name)+len(self._type)+len(self._exact)}

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format="%(message)s")
    e = ClassificationEngine()
    s = e.get_stats()
    print(f"\n{'='*60}\nClassification Engine Self-Test\n{'='*60}")
    print(f"Rules: {s['total']} total ({s['name_patterns']} name, {s['type_rules']} type, {s['object_field_rules']} exact)")

    print("\n--- Salesforce ---")
    for o,f,t in [("Contact","Email","email"),("Contact","FirstName","string"),("Contact","LastName","string"),
                   ("Contact","Birthdate","date"),("Contact","MailingStreet","string"),("Account","Phone","phone"),
                   ("Account","AnnualRevenue","currency"),("Account","Industry","string"),("Opportunity","Amount","currency"),
                   ("Lead","Email","email"),("Lead","Phone","phone"),("Case","Description","textarea")]:
        r = e.classify_field("salesforce",o,f,t)
        print(f"  {o}.{f} ({t}) -> {r or '(none)'}")

    print("\n--- Workday ---")
    for o,f,t in [("Workers","workerId","string"),("Workers","descriptor","string"),("Workers","primaryWorkEmail","email"),
                   ("Workers","primaryWorkPhone","phone"),("Workers","hireDate","date")]:
        r = e.classify_field("workday",o,f,t)
        print(f"  {o}.{f} ({t}) -> {r or '(none)'}")

    print("\n--- NetSuite ---")
    for o,f,t in [("customer","email","string"),("customer","phone","string"),("employee","email","string"),
                   ("salesOrder","total","currency"),("salesOrder","orderNumber","string")]:
        r = e.classify_field("netsuite",o,f,t)
        print(f"  {o}.{f} ({t}) -> {r or '(none)'}")

    print("\n--- Batch (Salesforce Contact) ---")
    flds = [{"name":"Email","type":"email"},{"name":"FirstName","type":"string"},{"name":"LastName","type":"string"},
            {"name":"Phone","type":"phone"},{"name":"Birthdate","type":"date"},{"name":"Department","type":"string"},{"name":"CreatedDate","type":"datetime"}]
    b = e.classify_fields("salesforce","Contact",flds)
    for fn,cl in b.items(): print(f"  {fn} -> {cl}")
    print(f"  ({len(flds)-len(b)} unclassified)")
    print(f"\n{'='*60}\nDone.\n{'='*60}")
