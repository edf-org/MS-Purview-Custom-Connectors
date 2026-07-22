**MICROSOFT PURVIEW**
 
Data Management Maturity
 
**Comprehensive Plan of Action**
 
*Module Linkages, Dependencies **&** Implementation Roadmap*
 
Version 1.0  |  March 2026
 
**CONFIDENTIAL – FOR INTERNAL USE ONLY**
 
# Table of Contents
 
**1. **Executive Summary
 
**2. **The Purview Ecosystem – Three Pillars
 
**3. **Master Dependency Map – How Every Module Connects
 
**4. **Implementation Roadmap (Six Phases)
 
**    Phase 1: **Foundation – Data Map & Source Registration
 
**    Phase 2: **Classification, Sensitivity Labels & Information Protection
 
**    Phase 3: **Enterprise Glossary, Governance Domains & Unified Catalog
 
**    Phase 4: **Data Products, OKRs & Data Quality
 
**    Phase 5: **Data Security – DLP, Insider Risk & DSPM
 
**    Phase 6: **Compliance, Lifecycle Management & Continuous Improvement
 
**5. **Cross-Phase Dependency Matrix
 
**6. **RACI Model for Data Governance Roles
 
**7. **Maturity Assessment Framework
 
**8. **Risk Register & Mitigation Strategies
 
**9. **Recommended Timeline & Quick Wins
 
**10. **Appendix – Licensing Considerations
 
**11. **Closing the Classification Gap – Custom Connector Strategy
 
**12. **DAMA DMBOK2 Framework Alignment
 
**13. **Best Practices from the Data Governance Handbook (Batchelder)
 
# 1. Executive Summary
 
Microsoft Purview is a comprehensive platform that unifies data governance, data security, and data compliance into a single management experience. Achieving data management maturity through Purview is not a matter of activating features in isolation; rather, it requires a carefully sequenced implementation where each module builds upon and reinforces the capabilities established before it.
 
This plan of action provides a phased, dependency-aware roadmap for deploying Microsoft Purview across your organisation. It establishes the Data Map as the foundational layer upon which classification, the Enterprise Glossary, data products, data quality, security policies, and compliance capabilities are progressively layered. Each phase includes explicit linkages to prior and subsequent phases, ensuring that no capability is deployed without its prerequisites in place.
 
**Core principle: **You cannot protect what you cannot see, you cannot govern what you have not classified, and you cannot create business value from data that lacks business context. This sequencing principle drives the entire plan.
 
# 2. The Purview Ecosystem – Three Pillars
 
Microsoft Purview operates across three interconnected pillars. Understanding this architecture is essential for appreciating why certain modules must precede others in any implementation plan.
 
## 2.1 Pillar 1: Data Governance (Azure Purview)
 
This pillar focuses on discovering, classifying, and managing data across on-premises, multi-cloud, and SaaS environments. The primary tools are the Data Map (for scanning and metadata capture) and the Unified Catalog (for curating, organising, and making data discoverable). This is the foundation upon which all other capabilities rest.
 
- **Data Map: **Registers sources, scans assets, captures metadata and lineage
 
- **Unified Catalog: **Governance domains, glossary terms, data products, OKRs, critical data elements, data quality, health controls
 
- **Data Quality: **Rules-based quality scanning, scoring, and remediation tracking
 
## 2.2 Pillar 2: Data Security (M365 Purview)
 
Evolved from Microsoft Information Protection (MIP), this pillar covers data loss prevention, insider risk management, information protection via sensitivity labels, adaptive protection, and the newer Data Security Posture Management (DSPM) capabilities.
 
- **Information Protection: **Sensitivity labels, auto-labelling, encryption
 
- **Data Loss Prevention (DLP): **Policy enforcement across M365, endpoints, and network
 
- **Insider Risk Management: **Behavioural analytics, risky agent detection
 
- **DSPM: **Unified posture view, AI observability, risk dashboards
 
## 2.3 Pillar 3: Data Compliance (M365 Purview)
 
This pillar addresses regulatory compliance and risk, including Compliance Manager (real-time scoring against frameworks such as GDPR, HIPAA, ISO 27001), eDiscovery and Audit, Communication Compliance, Data Lifecycle Management, and Records Management.
 
- **Compliance Manager: **Assessment scoring, improvement actions, regulatory templates
 
- **Data Lifecycle Management: **Retention policies, retention labels, disposition
 
- **Records Management: **File plan, regulatory records, disposition review
 
## 2.4 The Connecting Tissue
 
Several capabilities span all three pillars and serve as connectors: sensitivity labels and classifications flow from governance into security policies and compliance; lineage data from Data Map informs DLP scope and compliance audits; the Unified Catalog’s health scoring provides a single measure of maturity across governance, security, and compliance posture.
 
***Figure 1: ****The Microsoft Purview ecosystem showing all three pillars and their shared foundation layer.*
 
# 3. Master Dependency Map – How Every Module Connects
 
The following table illustrates the critical dependency chain across all major Purview modules. Reading left to right, each component requires its predecessor and enables its successor. This is the single most important reference in this document.
 
| **Component** | **Depends On** | **Feeds Into** | **Why This Sequence** |
| --- | --- | --- | --- |
| **Data Map (Source Registration ****&**** Scanning)** | — (Starting point) | Classification, Unified Catalog, Lineage | Without scanned metadata, no other module has data to work with |
| **Classifications ****&**** Sensitive Info Types** | Data Map scans | Sensitivity Labels, DLP Policies, Data Quality | Automated pattern detection identifies what needs protecting or governing |
| **Sensitivity Labels** | Classifications | DLP, Auto-labelling, Insider Risk, Encryption | Labels are the policy-enforcement vehicle; they carry classifications into action |
| **Governance Domains** | Data Map (source awareness) | Glossary Terms, Data Products, OKRs, Steward assignments | Domains create ownership boundaries; they scope who governs what |
| **Enterprise Glossary** | Governance Domains | Data Products (discoverability), Consumers (shared vocabulary) | Terms provide business context that makes technical metadata usable |
| **Data Products** | Data Map assets + Glossary Terms + Governance Domains | OKRs, Access Policies, Data Quality rules, Consumer self-service | Products bundle assets into use-case-oriented kits for consumption |
| **Critical Data Elements (CDEs)** | Data Products + Data Map columns | Data Quality (rules cascade), Standardisation | CDEs unify synonymous columns across systems under one logical name |
| **Data Quality** | Data Products, CDEs, Data Map connections | Health Controls, Trust signals, Remediation actions | Quality rules validate the data that products expose to consumers |
| **OKRs** | Governance Domains + Data Products | Health scoring, Business value tracking | OKRs connect data governance effort to measurable business outcomes |
| **Health Controls ****&**** Scoring** | All Unified Catalog components | Maturity measurement, Continuous improvement | Aggregated score across governance activities drives accountability |
| **DLP Policies** | Sensitivity Labels, Classifications | DSPM dashboards, Compliance Manager | Labels plus rules equal enforcement; DLP operationalises protection |
| **Insider Risk Management** | DLP signals, Sensitivity Labels, User activity | Adaptive Protection, DSPM | Behavioural analytics layer on top of policy-based protection |
| **DSPM** | DLP, Insider Risk, Information Protection, Classifications | Executive dashboards, AI Observability | Unified posture view consolidating all security signals |
| **Compliance Manager** | DLP, Labels, Retention, Audit logs | Regulatory reporting, Improvement actions | Measures compliance posture against regulatory frameworks |
| **Data Lifecycle Management** | Classifications, Sensitivity Labels | Records Management, Compliance Manager | Retention and disposition policies depend on knowing what data exists and its sensitivity |
| **Lineage** | Data Map scans across interconnected sources | Impact analysis, Compliance audits, Data Products | Shows end-to-end data journey from ingestion to consumption |
 
***Figure 2: ****Six-phase implementation roadmap with required and enriching dependencies, plus parallel execution windows.*
 
# 4. Implementation Roadmap – Six Phases
 
The roadmap is structured into six sequential phases. While some activities within adjacent phases can overlap, the phase sequence itself must be respected because each phase creates prerequisites for the next. Where parallel tracks are possible, these are explicitly noted.
 
| **PHASE 1** | **Foundation – Data Map ****&**** Source Registration** *"**See everything before you govern anything**"* | **Weeks 1–6** |
| --- | --- | --- |
 
### 4.1.1 Objective
 
Establish the foundational metadata layer by registering data sources, executing scans, and building an initial inventory of all data assets across your estate. This is the absolute prerequisite for every subsequent phase.
 
### 4.1.2 Key Activities
 
- **Provision Microsoft Purview Enterprise instance **(or upgrade from free tier). Assign the Data Governance Administrator role to the lead governance resource.
 
- **Inventory existing data sources **across Azure, on-premises, multi-cloud (AWS S3, Snowflake, Google Cloud), and SaaS (Power BI, Fabric). Prioritise by business criticality.
 
- **Register data sources in Data Map. **Start with the highest-value sources: the Power BI tenant (quick win for lineage), primary Azure SQL databases, ADLS Gen2 containers, and any Snowflake or S3 stores used by analytics teams.
 
- **Configure and run scans. **Choose scan levels: L1 (metadata only), L2 (schema), or L3 (schema + classification). Schedule recurring scans for dynamic sources.
 
- **Review scan outputs. **Validate that discovered assets, schemas, and automatic classifications are accurate. Assign initial asset owners.
 
- **Establish platform domains (collections) **in Data Map to organise scanned assets by technology or project alignment. Note: these are technical groupings, distinct from the governance (business) domains created in Phase 3.
 
### 4.1.3 Outputs & Deliverables
 
- Populated Data Map with metadata from all priority data sources
 
- Initial automated classifications applied to scanned assets
 
- Scan schedules configured for ongoing metadata freshness
 
- Platform domain structure defined in Data Map
 
- Asset ownership assignments for tier-1 data sources
 
### 4.1.4 Dependencies Created for Subsequent Phases
 
**Feeds Phase 2: **Scanned assets with initial classifications become the input for sensitivity labelling and refined classification rules.
 
**Feeds Phase 3: **Data Map assets are the raw material from which governance domains, glossary terms, and data products will be constructed in the Unified Catalog.
 
**Feeds Phase 5: **Asset visibility enables scoping of DLP policies and DSPM baselines.
 
| **PHASE 2** | **Classification, Sensitivity Labels ****&**** Information Protection** *"**Label what matters, protect what’s sensitive**"* | **Weeks 4–10** |
| --- | --- | --- |
 
### 4.2.1 Objective
 
Refine automated classifications, deploy sensitivity labels, and establish the labelling infrastructure that will underpin DLP, encryption, access controls, and compliance policies in later phases. This phase can overlap with the tail end of Phase 1 as initial scans complete.
 
### 4.2.2 Key Activities
 
- **Review and refine built-in classifications. **Purview includes over 300 built-in sensitive information types (SITs) such as credit card numbers, national IDs, and health information. Validate relevance; disable noisy classifiers; create custom SITs for organisation-specific patterns (e.g., internal project codes, employee identifiers).
 
- **Define the sensitivity label taxonomy. **Design a hierarchical label structure (e.g., Public > Internal > Confidential > Highly Confidential) aligned with your data classification policy. Include sub-labels where needed.
 
- **Configure label actions. **For each label, define enforcement actions: visual markings (headers/footers/watermarks), encryption (Azure RMS), access restrictions, and content marking.
 
- **Deploy auto-labelling policies. **Use the classifications from Data Map scans as conditions for auto-labelling. Start in simulation mode to validate accuracy before enforcement.
 
- **Publish labels to users. **Roll out manual labelling capability to pilot groups. Provide training on when and how to apply labels.
 
- **Extend labelling to structured data. **Apply sensitivity labels to Azure SQL columns, Power BI datasets, and Fabric lakehouses where supported.
 
### 4.2.3 Linkage to Phase 1 (Data Map)
 
The classifications automatically detected during Data Map scans are the primary trigger for sensitivity labels. Without Phase 1’s scanning output, there is nothing to label. Conversely, labels applied in this phase flow back into the Data Map’s metadata, enriching the asset inventory.
 
### 4.2.4 Dependencies Created for Subsequent Phases
 
**Feeds Phase 3: **Glossary term policies in the Unified Catalog can reference sensitivity labels, ensuring business vocabulary carries security implications.
 
**Feeds Phase 5: **DLP policies are built on top of sensitivity labels. Without a mature labelling infrastructure, DLP rules lack specificity and produce excessive false positives.
 
**Feeds Phase 6: **Compliance Manager improvement actions reference label coverage. Retention policies use labels as conditions for lifecycle management.
 
| **PHASE 3** | **Enterprise Glossary, Governance Domains ****&**** Unified Catalog** *"**Give data business meaning and ownership**"* | **Weeks 8–16** |
| --- | --- | --- |
 
### 4.3.1 Objective
 
Transform the technical metadata captured in the Data Map into a business-oriented, curated catalog. This is where the Enterprise Glossary fits into the overall plan: it provides the shared business vocabulary that bridges the gap between raw technical assets and meaningful, discoverable data products.
 
### 4.3.2 How the Enterprise Glossary Fits In
 
The glossary is not a standalone deliverable; it is the connective tissue between the technical layer (Data Map) and the business layer (Data Products and OKRs). Its role in the overall plan is threefold:
 
- **Vocabulary standardisation: **Business users search for data using terms like “Customer”, “Revenue”, or “Churn Rate” rather than table names like “dbo.fact_sales_monthly”. Glossary terms map business language to technical assets.
 
- **Active governance: **In the new Unified Catalog, glossary terms are no longer static definitions. They are “active objects” that can carry policies determining how associated data should be managed, governed, and made discoverable.
 
- **Discoverability amplifier: **When glossary terms are linked to data products, they dramatically improve search results. A consumer searching for “Vaccine Distribution” will find the relevant data product because the glossary term connects the concept to the underlying tables and reports.
 
***Figure 3: ****The Enterprise Glossary as the central bridge between technical metadata inputs and business-ready outputs.*
 
### 4.3.3 Key Activities
 
- **Create governance domains. **These are business-aligned boundaries (e.g., Finance, HR, Sales, Supply Chain, Customer Experience) that distribute ownership and governance responsibility. Do not mirror your Data Map’s platform domains (which are technology-aligned). Start with 3–5 domains where strong data stewardship already exists.
 
- **Assign domain roles. **Appoint governance domain owners, data stewards, and data product owners for each domain. These individuals should be business and data experts, not IT staff alone.
 
- **Build the enterprise glossary. **Begin with terms your teams already use. Many departments have informal glossaries for onboarding new staff. Formalise these into Unified Catalog glossary terms with clear definitions, owners, and related assets.
 
- **Create a catalog style guide. **Document standards for term definitions: what the term means, who owns it, when to use it, known caveats, related terms. Consistency is essential for trust.
 
- **Link glossary terms to Data Map assets. **As terms are published, attach them to the relevant scanned assets. This creates the bidirectional link between business vocabulary and technical metadata.
 
- **Publish the initial glossary and grant catalog reader access **to the teams who helped create it. Do not open the catalog to the entire organisation until data products are ready (Phase 4), as a catalog without curated products erodes user trust.
 
### 4.3.4 Critical Dependency Chain
 
This phase sits at the intersection of the technical and business layers:
 
- **From Phase 1 (Data Map): **Governance domains are scoped against scanned assets. Without knowing what data exists, you cannot assign meaningful domain boundaries or create accurate glossary term-to-asset mappings.
 
- **From Phase 2 (Labels): **Glossary terms can carry policies referencing sensitivity labels, meaning the business vocabulary inherits the security posture defined in Phase 2.
 
- **Into Phase 4 (Data Products): **Data products are built from scanned assets and enriched with glossary terms. The glossary must exist before products can be meaningfully curated.
 
| **PHASE 4** | **Data Products, OKRs ****&**** Data Quality** *"**Package data for consumption and measure its worth**"* | **Weeks 14–22** |
| --- | --- | --- |
 
### 4.4.1 Objective
 
Transform catalogued assets into consumable data products, establish critical data elements for cross-system standardisation, deploy data quality rules, and connect data governance effort to business outcomes through OKRs.
 
### 4.4.2 Key Activities
 
- **Create data products. **A data product bundles related assets (tables, files, Power BI reports) under a defined use case. For example, a “Sales Performance” data product might contain the fact_sales table, three dimension tables, and two Power BI reports. Data products are managed within a governance domain but discoverable across all domains.
 
- **Link glossary terms to data products. **This is where the Phase 3 glossary pays dividends: every data product gains business context through its associated terms, making it findable via business vocabulary.
 
- **Define critical data elements (CDEs). **CDEs group synonymous columns across systems into logical containers. For example, a “Customer ID” CDE maps “CustID” from System A and “CID” from System B. CDEs enable cross-system standardisation and can carry data quality rules and access policies.
 
- **Deploy data quality rules. **Attach rules to data products and CDEs. Use the Data Quality scanner (supporting Azure SQL, ADLS Gen2, Fabric, Snowflake, and SQL Managed Instance). Choose full or incremental scans based on data freshness requirements.
 
- **Create OKRs within governance domains. **Link OKRs to data products to connect governance effort to measurable business outcomes (e.g., “10% rise in sales forecast accuracy” linked to the Sales Performance data product).
 
- **Configure access policies for data products. **Use Unified Catalog access policies to enable self-service access requests with automated workflows (manager approval, attestation). This replaces manual, ad hoc access provisioning.
 
- **Endorse data products. **Product owners can flag products as “Endorsed” to signal quality certification, building consumer confidence.
 
- **Roll out Unified Catalog access to broader user base. **Now that products are curated and glossary terms are in place, extend catalog reader access to analysts, data scientists, and business users.
 
### 4.4.3 Linkages & Dependencies
 
- **Requires Phase 1: **Data products are built from Data Map assets. No assets = no products.
 
- **Requires Phase 3: **Glossary terms and governance domains are structural prerequisites for meaningful data product creation.
 
- **Feeds Phase 5: **Data quality scores inform risk posture in DSPM. Access policies interact with DLP controls.
 
- **Feeds Phase 6: **Health scores from data products contribute to compliance posture and Compliance Manager assessments.
 
| **PHASE 5** | **Data Security – DLP, Insider Risk ****&**** DSPM** *"**Enforce protection across the data lifecycle**"* | **Weeks 18–28** |
| --- | --- | --- |
 
### 4.5.1 Objective
 
Operationalise the data protection capabilities of Microsoft Purview by deploying DLP policies, activating Insider Risk Management, and establishing the Data Security Posture Management (DSPM) dashboard for unified risk visibility. This phase can begin overlapping with Phase 4.
 
### 4.5.2 Key Activities
 
- **Deploy DLP policies. **Build policies using sensitivity labels (Phase 2) as conditions. Start with M365 workloads (Exchange, SharePoint, OneDrive, Teams), then extend to endpoints and, where licensed, network traffic via Network Data Security for Shadow AI prevention.
 
- **Configure Insider Risk Management. **Activate policy templates for data theft, data leaks, and the new “Risky Agents” template for detecting anomalous behaviours in Copilot Studio and Microsoft Foundry agents.
 
- **Activate Adaptive Protection. **This connects DLP and Insider Risk by dynamically adjusting DLP policy strictness based on a user’s risk level. High-risk users (as scored by Insider Risk) automatically face more restrictive DLP controls.
 
- **Set up DSPM. **Data Security Posture Management consolidates insights from DLP, Insider Risk, Information Protection, and Data Security Investigations into a unified dashboard. Configure data security objectives: Prevent data exposure in Copilot interactions, Prevent oversharing of sensitive data, Prevent exfiltration to risky locations, Discover sensitive data.
 
- **Enable AI observability. **DSPM now provides agent-specific activity tracking for Copilot and third-party AI apps, including oversharing detection, exfiltration monitoring, and unusual access pattern analysis.
 
- **Deploy Security Copilot agents. **Automate alert triage for DLP and Insider Risk using the new Security Copilot agents, and use the DSPM Posture Agent for natural-language data searches.
 
### 4.5.3 Linkages & Dependencies
 
- **Requires Phase 1: **DLP scope is defined by the asset estate discovered in the Data Map.
 
- **Requires Phase 2: **DLP rules are conditioned on sensitivity labels. Without labels, DLP operates blindly.
 
- **Enriched by Phase 4: **Data quality scores and data product access patterns provide additional signals for DSPM risk assessment.
 
- **Feeds Phase 6: **DLP incident data, audit logs, and DSPM metrics feed into Compliance Manager scoring and regulatory reporting.
 
| **PHASE 6** | **Compliance, Lifecycle Management ****&**** Continuous Improvement** *"**Close the loop and sustain maturity**"* | **Weeks 24–36+** |
| --- | --- | --- |
 
### 4.6.1 Objective
 
Activate compliance and data lifecycle capabilities that depend on the full stack being in place. Establish continuous improvement mechanisms through health scoring, maturity assessments, and automated governance workflows.
 
### 4.6.2 Key Activities
 
- **Compliance Manager configuration: **Select relevant regulatory frameworks (GDPR, HIPAA, ISO 27001, SOX, CCPA). Review baseline compliance score. Address improvement actions, many of which map directly to DLP policies, label coverage, and retention settings deployed in earlier phases.
 
- **Data Lifecycle Management: **Deploy retention policies using sensitivity labels and classifications as conditions. Configure auto-retention for high-sensitivity data and disposition workflows for expired content.
 
- **Records Management: **Establish a file plan for regulatory records. Configure disposition review processes and regulatory record locks where required.
 
- **eDiscovery and Audit: **Configure audit log retention, eDiscovery cases, and search scopes. Link to DSPM investigation capabilities for enriched search.
 
- **Health Controls and scoring: **Use Unified Catalog health controls to track governance maturity. Edit control thresholds and assign ownership to controls. Establish a regular cadence of health reviews.
 
- **Automated governance workflows: **Now generally available in Unified Catalog, use workflows for data product access management and publication of data products and glossary terms, replacing manual approval chains.
 
- **Purview Analytics to OneLake: **Export governance analytics from Unified Catalog into OneLake for deeper analysis in Microsoft Fabric.
 
# 5. Cross-Phase Dependency Matrix
 
The following matrix summarises the inter-phase dependencies at a glance. An “R” indicates the column phase is Required by the row phase. An “E” indicates it is Enriched by (benefits from but does not strictly require). A blank cell indicates no direct dependency.
 
| **Phase** | **P1 Data Map** | **P2 Labels** | **P3 Glossary** | **P4 Products** | **P5 Security** | **P6 Compliance** |
| --- | --- | --- | --- | --- | --- | --- |
| **P1: Data Map** | — |  |  |  |  |  |
| **P2: Labels** | **R** | — |  |  |  |  |
| **P3: Glossary** | **R** | E | — |  |  |  |
| **P4: Products** | **R** | E | **R** | — |  |  |
| **P5: Security** | **R** | **R** | E | E | — |  |
| **P6: Compliance** | **R** | **R** | E | E | **R** | — |
 
**R = Required dependency  **|  **E = Enriching dependency**
 
# 6. RACI Model for Data Governance Roles
 
Microsoft Purview’s federated governance model requires clear role assignments. The following RACI matrix maps activities to the five key personas defined by Microsoft for Purview data governance.
 
| **Activity** | **Responsible** | **Accountable** | **Consulted** | **Informed** |
| --- | --- | --- | --- | --- |
| **Data Map source registration** | Data Owner | Data Gov Admin | IT / Platform | Central Data Office |
| **Scan configuration ****&**** scheduling** | IT / Platform | Data Gov Admin | Data Owner | Data Steward |
| **Classification review ****&**** refinement** | Data Steward | Data Gov Admin | Data Owner | Consumers |
| **Sensitivity label design** | Central Data Office | CISO / DPO | IT Security | Data Owners |
| **Governance domain creation** | Central Data Office | Data Gov Admin | Domain Owners | All users |
| **Glossary term curation** | Data Steward | Domain Owner | Data Consumers | Central Data Office |
| **Data product creation** | Data Product Owner | Domain Owner | Data Steward | Consumers |
| **CDE definition ****&**** quality rules** | Data Steward | Data Quality Steward | Data Product Owner | Consumers |
| **Data quality monitoring** | Data Quality Steward | Domain Owner | Data Product Owner | Central Data Office |
| **DLP policy deployment** | IT Security | CISO | Central Data Office | Data Owners |
| **Insider Risk configuration** | IT Security | CISO | HR / Legal | Central Data Office |
| **DSPM dashboard management** | IT Security | CISO | Data Gov Admin | Executive Sponsors |
| **Compliance Manager reviews** | Compliance Team | DPO / CISO | Central Data Office | Executive Sponsors |
| **Health control monitoring** | Data Health Reader | Data Gov Admin | Domain Owners | All users |
| **OKR definition ****&**** tracking** | Business Strategy | Domain Owner | Data Steward | Executive Sponsors |
 
# 7. Maturity Assessment Framework
 
Use the following five-level maturity model to assess progress and identify gaps. Each level corresponds roughly to the phases in this plan.
 
***Figure 4: ****Data management maturity staircase mapping each level to corresponding implementation phases.*
 
| **Level** | **Name** | **Description** | **Purview Modules Active** |
| --- | --- | --- | --- |
| **1** | **Initial / Ad Hoc** | Data management is reactive. No central inventory. Sensitive data locations unknown. Classification is manual or non-existent. | None or Data Map (partial) |
| **2** | **Managed** | Data sources are registered and scanned. Automated classifications are in place. Sensitivity labels are deployed. Initial governance roles assigned. | Data Map, Classifications, Sensitivity Labels |
| **3** | **Defined** | Governance domains, enterprise glossary, and data products are established. Business users can discover data through the Unified Catalog. Data quality rules are deployed. | Above + Unified Catalog, Glossary, Data Products, CDEs, Data Quality |
| **4** | **Measured** | DLP and Insider Risk policies enforce protection. DSPM provides unified posture visibility. OKRs connect governance to business outcomes. Health scoring is active. | Above + DLP, Insider Risk, DSPM, OKRs, Health Controls |
| **5** | **Optimised** | Continuous improvement via automated workflows, Compliance Manager, full lifecycle management, and AI-driven governance. Governance is embedded in daily operations. | Full Purview suite active, automated workflows, AI observability |
 
# 8. Risk Register & Mitigation Strategies
 
| **Risk** | **Likelihood** | **Impact** | **Mitigation** |
| --- | --- | --- | --- |
| Insufficient scan coverage leaves blind spots in the Data Map | Medium | High | Start with highest-value sources; establish a quarterly scan coverage review; use the Data Map’s source status dashboard to track gaps |
| Glossary terms remain unused or disconnected from data products | High | Medium | Mandate linking terms to data products before publication; review orphaned terms monthly; involve business stewards, not just IT |
| Users lose trust in the Unified Catalog due to poor data quality | Medium | High | Deploy data quality rules before granting broad catalog access; endorse products only after quality thresholds are met |
| DLP policies generate excessive false positives | High | Medium | Start all DLP policies in simulation mode; tune sensitivity label conditions before enforcement; phase rollout by workload |
| Licensing gaps prevent activation of required modules | Medium | High | Conduct a licensing pre-assessment mapping Purview features to E3/E5/add-on requirements before project kickoff |
| Organisational resistance to federated governance model | High | High | Secure executive sponsorship; start with domains where stewardship already exists; demonstrate quick wins before expanding |
| Data Map and Unified Catalog domains misaligned | Medium | Medium | Clearly document that platform domains (Data Map) are technical while governance domains (Unified Catalog) are business-aligned; provide mapping between the two |
| AI/Copilot governance gaps emerge before DSPM is active | Medium | High | Prioritise DSPM setup if Copilot is already deployed; use interim sensitivity labels on high-risk content while full posture management is established |
 
# 9. Recommended Timeline & Quick Wins
 
## 9.1 Overall Timeline
 
| **Phase** | **Duration** | **Parallel Opportunities** | **Key Milestone** |
| --- | --- | --- | --- |
| **P1: Data Map** | Weeks 1–6 | Begin P2 label design in Week 4 | Data Map populated with tier-1 sources |
| **P2: Labels** | Weeks 4–10 | Overlap with P1 tail; begin P3 domain planning in Week 8 | Auto-labelling live in simulation mode |
| **P3: Glossary ****&**** Catalog** | Weeks 8–16 | Overlap with P2; begin P4 product planning in Week 14 | First glossary terms published and linked to assets |
| **P4: Data Products** | Weeks 14–22 | Overlap with P3 tail; begin P5 DLP planning in Week 18 | First data products published with quality scores |
| **P5: Security** | Weeks 18–28 | Overlap with P4; begin P6 compliance planning in Week 24 | DLP policies enforcing; DSPM dashboard active |
| **P6: Compliance ****&**** Maturity** | Weeks 24–36+ | Continuous improvement begins | Compliance Manager score baselined; health controls active |
 
## 9.2 Quick Wins (First 30 Days)
 
These early deliverables build momentum and demonstrate value before the full roadmap is complete:
 
- **Week 1: **Create Purview account, assign Data Governance Administrator, register Power BI tenant and one primary Azure SQL database.
 
- **Week 2: **Run first scans, review automated classifications, add owners and glossary terms to the top 10 highest-value assets.
 
- **Week 3: **Create first governance domain aligned to the most data-mature business unit. Assign initial stewards.
 
- **Week 4: **Draft sensitivity label taxonomy. Begin auto-labelling simulation on one M365 workload.
 
# 10. Appendix – Licensing Considerations
 
Purview is not a single SKU. Different capabilities require different licensing tiers. Conduct a licensing assessment early to avoid discovering mid-implementation that a required module is not covered.
 
| **Capability Area** | **Typical License** | **Notes** |
| --- | --- | --- |
| Data Map, Unified Catalog, Data Quality | Azure consumption model | Billed per capacity unit based on scans, storage, and operations; start with one capacity unit and autoscale |
| Sensitivity Labels, Basic DLP | Microsoft 365 E3 (basic) or E5 (full) | E3 provides manual labelling; E5 adds auto-labelling, advanced DLP, and endpoint DLP |
| Full DLP, Insider Risk, Adaptive Protection | Microsoft 365 E5 or E5 Compliance add-on | E5 required for advanced insider risk and adaptive protection features |
| DSPM, AI Observability | E5 Security + Data Security Investigation Compute Units | Compute Units are the billing mechanism for AI-powered investigation capacity |
| Compliance Manager (advanced) | Microsoft 365 E5 Compliance | Basic compliance scoring available in E3; premium assessments require E5 |
| eDiscovery (Premium) | Microsoft 365 E5 or E5 eDiscovery add-on | Standard eDiscovery available in E3; premium features require E5 |
 
# 11. Closing the Classification Gap – Custom Connector Strategy
 
Purview’s native Salesforce, NetSuite, and Workday connectors extract metadata (objects, fields, schemas) but do not support automated data classification (no L3 scan). Salesforce does not appear as a source type in custom scan rule sets. This is confirmed platform behaviour, not a configuration issue.
 
**Solution: **Custom Python connectors bypass the native scanner entirely. They pull metadata directly from each source’s REST API, then push classifications, lineage, and business metadata into Purview via the Atlas v2 API. The result in the Unified Catalog is indistinguishable from a native L3 scan.
 
## 11.1 Available Connectors
 
- **purview_salesforce_connector.py: **Salesforce CRM — OAuth 2.0, object/field discovery, classifications via engine, cross-system lineage
 
- **purview_netsuite_connector.py: **Oracle NetSuite — OAuth 1.0a (TBA), record type/field discovery, classifications via engine, lineage to DW/BI
 
- **purview_workday_connector.py: **Workday HCM — OAuth 2.0 with refresh tokens, business object discovery, classifications via engine, lineage to AD
 
- **purview_sql_connector.py: **SQL Server — Service Principal auth, DB → schema → table → column hierarchy, classifications via engine, lineage
 
## 11.2 Configuration-Driven Classification Engine
 
Rather than hardcoding classification rules in each connector’s Python code, all four connectors use a shared Classification Engine that separates “what to classify” (maintained by data stewards in a JSON file) from “how to classify” (handled by a reusable Python module).
 
- **classification_rules.json: **52 rules across three priority layers. Object-field exact rules (priority 50) beat field name patterns (priority 10) which beat type rules (priority 5). Data stewards add, modify, or disable rules without touching Python code.
 
- **classification_engine.py: **Shared Python module imported by all four connectors. Loads the JSON, evaluates all rule layers, returns the winning Purview classification type. Includes built-in self-test.
 
**Important limitation: **This is rule-based classification (matching on field names, types, and object context), not content-based classification (inspecting actual data values). For content-level detection within Salesforce, complement with Salesforce Shield Data Detect or Microsoft Defender for Cloud Apps.
 
# 12. DAMA DMBOK2 Framework Alignment
 
The following table maps all 11 DAMA knowledge areas to specific Purview modules and roadmap phases. Coverage ratings reflect the custom connector strategy: areas previously rated “Partial” for SaaS sources have been upgraded now that custom connectors provide full classification and lineage capabilities.
 
| **DAMA Knowledge Area** | **Microsoft Purview Module** | **Phase** | **Coverage** |
| --- | --- | --- | --- |
| **Data Governance (Hub)** | Unified Catalog, Governance Domains, Health Controls | P3, P6 | **Full** |
| **Data Architecture** | Data Map (source registration, collections, platform domains) | P1 | **Partial** |
| **Data Modeling ****&**** Design** | Data Map (schema capture), Lineage visualisation | P1 | **Partial** |
| **Data Storage ****&**** Operations** | Data Map (native + custom connectors: Azure, AWS, GCP, Salesforce, NetSuite, Workday) | P1 | **Moderate** |
| **Data Security** | Info Protection, DLP, Insider Risk, DSPM, Adaptive Protection | P2, P5 | **Full** |
| **Data Integration ****&**** Interoperability** | Data Map (lineage) + Custom connectors (cross-system lineage via Atlas v2) | P1, P4 | **Full** |
| **Document ****&**** Content Mgmt** | Sensitivity Labels on M365 (SharePoint, OneDrive, Teams) | P2, P6 | **Moderate** |
| **Reference ****&**** Master Data** | Critical Data Elements (CDEs), Data Products | P4 | **Moderate** |
| **Data Warehousing ****&**** BI** | Data Map (Power BI, Fabric connectors), Purview Analytics to OneLake | P1, P6 | **Moderate** |
| **Metadata Management** | Data Map + Custom connectors (SaaS metadata) + Enterprise Glossary | P1, P3 | **Full** |
| **Data Quality** | Data Quality scanner, Health Controls, CDE quality rules | P4, P6 | **Full** |
 
**Full** = Purview (native + custom connectors) provides comprehensive capability  |  **Moderate** = Purview covers key aspects; custom connectors extend reach  |  **Partial** = Metadata visibility only; dedicated design tools needed
 
**Remaining gaps: **Custom connectors have closed the SaaS classification and lineage gaps. Data Architecture and Data Modeling remain the only areas where Purview provides metadata visibility but not design tooling. Pair with dedicated tools (e.g., erwin, ER/Studio) for complete DAMA coverage.
 
*Framework: DAMA-DMBOK2R, DAMA International, 2024.*
 
# 13. Best Practices from the Data Governance Handbook
 
The following recommendations are drawn from Wendy S. Batchelder’s Data Governance Handbook (Packt, 2024) and integrated into our Purview roadmap.
 
## 13.1 Data as Asset and Liability
 
Batchelder frames data through a dual lens: data is an asset when it creates value (curated datasets, golden sources, predictive models) and a liability when it creates risk (uncatalogued data, uncontrolled access, unknown locations). Purview Action: Use Data Map scanning to surface liabilities; endorse Data Products in Unified Catalog to signal certified assets.
 
## 13.2 Build a Coalition of Advocates
 
Governance programmes fail without executive sponsorship and stakeholder buy-in. Key steps: (1) Land an executive sponsor with authority and funding. (2) Build a stakeholder map identifying primary, secondary, and tertiary stakeholders. (3) Focus on trust, not “data culture” — Batchelder argues culture programmes often fail; trust-building through delivery is more effective. (4) Translate everything into business outcomes: revenue, cost savings, or risk reduction.
 
## 13.3 Baseline Before You Build
 
Run a data management maturity assessment before starting the Purview deployment. Batchelder’s 10-step process (define scope, identify stakeholders, select model, execute assessment, analyse, communicate, plan, implement, monitor, reassess) maps directly to our Phase 1 and ongoing Phase 6 activities. Use aggregated enterprise scores to unify the organisation rather than disaggregated scores that create silos.
 
## 13.4 Quick Wins and Continuous Delivery
 
Data leaders who bet exclusively on big transformational platforms often lose stakeholder interest before results are realised. Shorten time to first value through a continuous delivery flywheel: Intake → Prioritise → Build MVP → Demo & Iterate → Go Live → Communicate. Apply a product mindset to data capabilities — go live with an MVP and iterate, rather than waiting for perfection.
 
## 13.5 Metadata, Glossary & Trust
 
Metadata management is the foundational capability upon which all others depend. Start the business glossary with terms teams already use. Measure value by employee time saved — analysts spend 50%+ of time searching for data. The Unified Catalog in Purview fulfils Batchelder’s vision of a “Data Marketplace” combining glossary, catalog, lineage, quality, and certified assets in one place.
 
## 13.6 Adoption and Governance by Design
 
Governance must be embedded into process, not bolted on. Batchelder calls this “governance by design.” In Purview, this manifests as: auto-labelling embedding classification into document creation (Phase 2), glossary terms carrying active policies that flow to data products (Phase 3), access request workflows replacing ad-hoc provisioning (Phase 4), and DSPM + Adaptive Protection auto-adjusting security posture (Phase 5).
 
*Source: Data Governance Handbook, Wendy S. Batchelder, Packt Publishing, 2024.*
 
**End of Document. ***This plan should be reviewed quarterly and updated as Microsoft Purview capabilities evolve and your organisation’s maturity advances through each phase.*
