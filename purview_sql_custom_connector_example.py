"""
Microsoft Purview Custom Connector - Example Implementation
============================================================

This example demonstrates a complete custom connector for SQL Server that:
1. Authenticates to Purview using a Service Principal
2. Registers custom type definitions
3. Creates entities (database → schema → table → column hierarchy)
4. Builds lineage between source and destination tables
5. Applies business metadata and classifications

Prerequisites:
    pip install pyapacheatlas azure-identity python-dotenv requests

Environment Variables (set in .env file or system environment):
    AZURE_TENANT_ID       - Azure AD tenant ID
    AZURE_CLIENT_ID       - Service Principal application ID
    AZURE_CLIENT_SECRET   - Service Principal client secret
    PURVIEW_ACCOUNT_NAME  - Purview account name (without .purview.azure.com)

Usage:
    python purview_custom_connector_example.py
"""

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

# Classification engine — loads rules from classification_rules.json
from classification_engine import ClassificationEngine

# --- Uncomment these imports when running against a real Purview account ---
# from pyapacheatlas.auth import ServicePrincipalAuthentication
# from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess
# from pyapacheatlas.core.typedef import (
#     AtlasAttributeDef,
#     EntityTypeDef,
#     RelationshipTypeDef,
# )
# from azure.identity import DefaultAzureCredential
# from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Security: default timeout for all HTTP requests (seconds) ---
REQUEST_TIMEOUT = (10, 30)


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

@dataclass
class PurviewConfig:
    """Configuration for connecting to Microsoft Purview."""
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    account_name: str = ""

    @classmethod
    def from_environment(cls) -> "PurviewConfig":
        """Load configuration from environment variables."""
        # load_dotenv()  # Uncomment to load from .env file
        return cls(
            tenant_id=os.environ.get("AZURE_TENANT_ID", ""),
            client_id=os.environ.get("AZURE_CLIENT_ID", ""),
            client_secret=os.environ.get("AZURE_CLIENT_SECRET", ""),
            account_name=os.environ.get("PURVIEW_ACCOUNT_NAME", ""),
        )

    @property
    def endpoint(self) -> str:
        return f"https://{self.account_name}.purview.azure.com/datamap"


# =============================================================================
# 2. AUTHENTICATION SERVICE
# =============================================================================

class AuthService:
    """Handles authentication to Microsoft Purview.

    Supports two modes:
    - Service Principal (for local dev / non-Azure environments)
    - Managed Identity via DefaultAzureCredential (for Azure-hosted deployments)
    """

    def __init__(self, config: PurviewConfig):
        self.config = config

    def get_purview_client(self):  # -> PurviewClient
        """Create an authenticated PurviewClient using pyapacheatlas."""
        # --- Uncomment for real usage ---
        # auth = ServicePrincipalAuthentication(
        #     tenant_id=self.config.tenant_id,
        #     client_id=self.config.client_id,
        #     client_secret=self.config.client_secret,
        # )
        # return PurviewClient(
        #     account_name=self.config.account_name,
        #     authentication=auth,
        # )
        logger.info(f"[DRY RUN] Would authenticate to Purview account: {self.config.account_name}")
        return None

    def get_bearer_token(self) -> str:
        """Get a bearer token for direct REST API calls."""
        # --- Uncomment for real usage ---
        # credential = DefaultAzureCredential()
        # token = credential.get_token("https://purview.azure.net/.default")
        # return token.token
        logger.info("[DRY RUN] Would acquire bearer token via DefaultAzureCredential")
        return "dry-run-token"


# =============================================================================
# 3. TYPE DEFINITION SERVICE
# =============================================================================

class TypeDefService:
    """Manages custom type definitions in Purview.

    Custom types must be registered before creating entities of that type.
    Types are idempotent — re-registering an existing type updates it.
    """

    # Example custom type definitions for a SQL-like source
    CUSTOM_TYPES = {
        "entityDefs": [
            {
                "category": "ENTITY",
                "name": "custom_sql_server",
                "description": "A custom SQL Server instance",
                "superTypes": ["Server"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "serverVersion",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                    {
                        "name": "environment",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                        "description": "Environment: dev, staging, production",
                    },
                ],
            },
            {
                "category": "ENTITY",
                "name": "custom_sql_database",
                "description": "A database within a custom SQL Server",
                "superTypes": ["DataSet"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "databaseEngine",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                ],
            },
            {
                "category": "ENTITY",
                "name": "custom_sql_table",
                "description": "A table within a custom SQL database",
                "superTypes": ["DataSet"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "rowCount",
                        "typeName": "long",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                    {
                        "name": "schemaName",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                ],
            },
            {
                "category": "ENTITY",
                "name": "custom_sql_column",
                "description": "A column within a custom SQL table",
                "superTypes": ["DataSet"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "dataType",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                    },
                    {
                        "name": "isNullable",
                        "typeName": "boolean",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                    },
                    {
                        "name": "isPrimaryKey",
                        "typeName": "boolean",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                    },
                ],
            },
            {
                "category": "ENTITY",
                "name": "custom_sql_process",
                "description": "A data transformation process (ETL, stored proc, etc.)",
                "superTypes": ["Process"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {
                        "name": "processType",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": True,
                        "description": "Type of process: ETL, StoredProcedure, View, etc.",
                    },
                    {
                        "name": "queryText",
                        "typeName": "string",
                        "isOptional": True,
                        "cardinality": "SINGLE",
                        "isUnique": False,
                        "isIndexable": False,
                    },
                ],
            },
        ]
    }

    def __init__(self, client):
        self.client = client

    def register_types(self) -> dict:
        """Register all custom type definitions in Purview.

        Types are idempotent — if they already exist, they are updated.
        """
        logger.info("Registering custom type definitions...")

        # --- Uncomment for real usage ---
        # import requests
        # token = auth_service.get_bearer_token()
        # headers = {
        #     "Authorization": f"Bearer {token}",
        #     "Content-Type": "application/json",
        # }
        # response = requests.post(
        #     f"{config.endpoint}/api/atlas/v2/types/typedefs",
        #     headers=headers,
        #     json=self.CUSTOM_TYPES,
        #     timeout=REQUEST_TIMEOUT,
        # )
        # response.raise_for_status()
        # return response.json()

        logger.info(f"[DRY RUN] Would register {len(self.CUSTOM_TYPES['entityDefs'])} type definitions:")
        for td in self.CUSTOM_TYPES["entityDefs"]:
            logger.info(f"  - {td['name']} (superType: {td['superTypes'][0]})")
        return self.CUSTOM_TYPES


# =============================================================================
# 4. ENTITY SERVICE
# =============================================================================

@dataclass
class SourceAsset:
    """Represents a discovered asset from the source system."""
    name: str
    qualified_name: str
    entity_type: str
    attributes: dict = field(default_factory=dict)
    classifications: list = field(default_factory=list)


class EntityService:
    """Creates and manages entities in Purview.

    Uses the bulk entity API for efficient batch creation.
    Entities are upserted based on qualifiedName.
    """

    BATCH_SIZE = 50  # Max entities per bulk API call

    def __init__(self, client, config: PurviewConfig):
        self.client = client
        self.config = config

    def build_entity(self, asset: SourceAsset) -> dict:
        """Convert a SourceAsset into an Atlas entity payload."""
        entity = {
            "typeName": asset.entity_type,
            "attributes": {
                "qualifiedName": asset.qualified_name,
                "name": asset.name,
                **asset.attributes,
            },
            "status": "ACTIVE",
        }
        if asset.classifications:
            entity["classifications"] = [
                {"typeName": c} for c in asset.classifications
            ]
        return entity

    def create_entities_bulk(self, assets: list[SourceAsset]) -> list[dict]:
        """Create or update entities in Purview in batches.

        Returns list of API responses (one per batch).
        """
        results = []
        for i in range(0, len(assets), self.BATCH_SIZE):
            batch = assets[i : i + self.BATCH_SIZE]
            entities = [self.build_entity(a) for a in batch]
            payload = {"entities": entities}

            logger.info(f"Creating batch of {len(entities)} entities (batch {i // self.BATCH_SIZE + 1})...")

            # --- Uncomment for real usage ---
            # import requests
            # token = auth_service.get_bearer_token()
            # headers = {
            #     "Authorization": f"Bearer {token}",
            #     "Content-Type": "application/json",
            # }
            # response = requests.post(
            #     f"{self.config.endpoint}/api/atlas/v2/entity/bulk",
            #     headers=headers,
            #     json=payload,
            #     timeout=REQUEST_TIMEOUT,
            # )
            # response.raise_for_status()
            # results.append(response.json())

            # --- Alternative using pyapacheatlas ---
            # atlas_entities = [
            #     AtlasEntity(
            #         name=a.name,
            #         typeName=a.entity_type,
            #         qualified_name=a.qualified_name,
            #         attributes=a.attributes,
            #     )
            #     for a in batch
            # ]
            # result = self.client.upload_entities(atlas_entities)
            # results.append(result)

            logger.info(f"[DRY RUN] Would create {len(entities)} entities:")
            for e in entities:
                logger.info(f"  - [{e['typeName']}] {e['attributes']['qualifiedName']}")
            results.append(payload)

        return results


# =============================================================================
# 5. LINEAGE SERVICE
# =============================================================================

@dataclass
class LineageRelationship:
    """Defines a lineage relationship: inputs → process → outputs."""
    process_name: str
    process_qualified_name: str
    process_type: str  # e.g., "ETL", "StoredProcedure"
    input_qualified_names: list[str]
    output_qualified_names: list[str]
    input_type: str = "custom_sql_table"
    output_type: str = "custom_sql_table"
    query_text: Optional[str] = None


class LineageService:
    """Creates lineage relationships in Purview.

    Lineage is modeled as a Process entity with inputs and outputs.
    Each Process entity connects source entities to destination entities.
    """

    def __init__(self, client, config: PurviewConfig):
        self.client = client
        self.config = config

    def create_lineage(self, relationship: LineageRelationship) -> dict:
        """Create a lineage process entity linking inputs to outputs.

        Uses the bulk entity API with a Process-typed entity that references
        input and output entities by their qualifiedName.
        """
        logger.info(
            f"Creating lineage: {len(relationship.input_qualified_names)} inputs → "
            f"[{relationship.process_name}] → {len(relationship.output_qualified_names)} outputs"
        )

        # Build the process entity with input/output references
        process_entity = {
            "typeName": "custom_sql_process",
            "attributes": {
                "qualifiedName": relationship.process_qualified_name,
                "name": relationship.process_name,
                "processType": relationship.process_type,
                "inputs": [
                    {
                        "typeName": relationship.input_type,
                        "uniqueAttributes": {"qualifiedName": qn},
                    }
                    for qn in relationship.input_qualified_names
                ],
                "outputs": [
                    {
                        "typeName": relationship.output_type,
                        "uniqueAttributes": {"qualifiedName": qn},
                    }
                    for qn in relationship.output_qualified_names
                ],
            },
            "status": "ACTIVE",
        }

        if relationship.query_text:
            process_entity["attributes"]["queryText"] = relationship.query_text

        payload = {"entities": [process_entity]}

        # --- Uncomment for real usage ---
        # import requests
        # token = auth_service.get_bearer_token()
        # headers = {
        #     "Authorization": f"Bearer {token}",
        #     "Content-Type": "application/json",
        # }
        # response = requests.post(
        #     f"{self.config.endpoint}/api/atlas/v2/entity/bulk",
        #     headers=headers,
        #     json=payload,
        #     timeout=REQUEST_TIMEOUT,
        # )
        # response.raise_for_status()
        # return response.json()

        # --- Alternative using pyapacheatlas ---
        # process = AtlasProcess(
        #     name=relationship.process_name,
        #     typeName="custom_sql_process",
        #     qualified_name=relationship.process_qualified_name,
        #     inputs=[
        #         AtlasEntity(
        #             name=qn.split("/")[-1],
        #             typeName=relationship.input_type,
        #             qualified_name=qn,
        #         )
        #         for qn in relationship.input_qualified_names
        #     ],
        #     outputs=[
        #         AtlasEntity(
        #             name=qn.split("/")[-1],
        #             typeName=relationship.output_type,
        #             qualified_name=qn,
        #         )
        #         for qn in relationship.output_qualified_names
        #     ],
        #     attributes={"processType": relationship.process_type},
        # )
        # return self.client.upload_entities([process])

        logger.info("[DRY RUN] Would create lineage process entity:")
        logger.info(f"  Process: {relationship.process_qualified_name}")
        for qn in relationship.input_qualified_names:
            logger.info(f"  Input:   {qn}")
        for qn in relationship.output_qualified_names:
            logger.info(f"  Output:  {qn}")
        return payload


# =============================================================================
# 6. BUSINESS METADATA SERVICE
# =============================================================================

class MetadataService:
    """Manages business metadata and classifications on entities."""

    def __init__(self, config: PurviewConfig):
        self.config = config

    def apply_business_metadata(self, entity_guid: str, metadata: dict) -> dict:
        """Apply business metadata key-value pairs to an entity.

        Args:
            entity_guid: The GUID of the target entity.
            metadata: Dict of {business_metadata_name: {attr_name: attr_value}}.

        Example metadata:
            {
                "DataQuality": {
                    "lastValidated": "2026-01-15",
                    "qualityScore": 95,
                    "dataOwner": "analytics-team@company.com"
                }
            }
        """
        logger.info(f"Applying business metadata to entity {entity_guid}...")

        # --- Uncomment for real usage ---
        # import requests
        # token = auth_service.get_bearer_token()
        # headers = {
        #     "Authorization": f"Bearer {token}",
        #     "Content-Type": "application/json",
        # }
        # response = requests.post(
        #     f"{self.config.endpoint}/api/atlas/v2/entity/guid/{entity_guid}"
        #     f"/businessmetadata?isOverwrite=true",
        #     headers=headers,
        #     json=metadata,
        #     timeout=REQUEST_TIMEOUT,
        # )
        # response.raise_for_status()
        # return response.json()

        logger.info(f"[DRY RUN] Would apply business metadata to {entity_guid}:")
        logger.info(f"  {json.dumps(metadata, indent=2)}")
        return metadata

    def apply_classifications(self, entity_guid: str, classification_names: list[str]) -> dict:
        """Apply classification labels to an entity.

        Args:
            entity_guid: The GUID of the target entity.
            classification_names: List of classification type names (e.g., ["PII", "Confidential"]).
        """
        logger.info(f"Applying {len(classification_names)} classifications to entity {entity_guid}...")

        classifications = [{"typeName": name} for name in classification_names]

        # --- Uncomment for real usage ---
        # import requests
        # token = auth_service.get_bearer_token()
        # headers = {
        #     "Authorization": f"Bearer {token}",
        #     "Content-Type": "application/json",
        # }
        # response = requests.post(
        #     f"{self.config.endpoint}/api/atlas/v2/entity/guid/{entity_guid}/classifications",
        #     headers=headers,
        #     json=classifications,
        #     timeout=REQUEST_TIMEOUT,
        # )
        # response.raise_for_status()
        # return response.json()

        logger.info(f"[DRY RUN] Would apply classifications: {classification_names}")
        return {"classifications": classifications}


# =============================================================================
# 7. EXAMPLE CONNECTOR - SQL SERVER
# =============================================================================

class SQLServerConnector:
    """Example connector that discovers metadata from a SQL Server
    and pushes it to Purview.

    In a real implementation, this would query SQL Server's
    INFORMATION_SCHEMA to discover databases, schemas, tables, and columns.
    """

    SOURCE_TYPE = "custom_sql"

    def __init__(self, server_name: str, config: PurviewConfig):
        self.server_name = server_name
        self.config = config

    def _qualified_name(self, *parts: str) -> str:
        """Build a consistent qualifiedName for an asset."""
        path = "/".join(parts)
        return f"{self.SOURCE_TYPE}://{self.server_name}/{path}"

    def discover_assets(self) -> list[SourceAsset]:
        """Discover assets from the source system.

        In production, this would connect to the source and query metadata.
        Here we return sample data for demonstration.
        """
        logger.info(f"Discovering assets from {self.server_name}...")

        assets = []

        # Server
        assets.append(SourceAsset(
            name=self.server_name,
            qualified_name=self._qualified_name(),
            entity_type="custom_sql_server",
            attributes={"serverVersion": "SQL Server 2022", "environment": "production"},
        ))

        # Database
        db_name = "SalesDB"
        assets.append(SourceAsset(
            name=db_name,
            qualified_name=self._qualified_name(db_name),
            entity_type="custom_sql_database",
            attributes={"databaseEngine": "MSSQL"},
        ))

        # Source tables
        tables = [
            {"name": "Orders", "schema": "dbo", "rows": 1500000},
            {"name": "Customers", "schema": "dbo", "rows": 50000},
            {"name": "Products", "schema": "dbo", "rows": 2000},
        ]
        for t in tables:
            assets.append(SourceAsset(
                name=t["name"],
                qualified_name=self._qualified_name(db_name, t["schema"], t["name"]),
                entity_type="custom_sql_table",
                attributes={"rowCount": t["rows"], "schemaName": t["schema"]},
            ))

        # Destination (aggregated) table
        assets.append(SourceAsset(
            name="OrderSummary",
            qualified_name=self._qualified_name(db_name, "analytics", "OrderSummary"),
            entity_type="custom_sql_table",
            attributes={"rowCount": 365000, "schemaName": "analytics"},
        ))

        # Columns for Orders table (demonstrating column-level metadata)
        # Classifications now driven by classification_rules.json via ClassificationEngine
        classification_engine = ClassificationEngine()
        columns = [
            {"name": "OrderID", "type": "int", "nullable": False, "pk": True},
            {"name": "CustomerID", "type": "int", "nullable": False, "pk": False},
            {"name": "OrderDate", "type": "datetime", "nullable": False, "pk": False},
            {"name": "TotalAmount", "type": "decimal(18,2)", "nullable": True, "pk": False},
        ]

        # Classify all columns in the table using the shared engine
        col_classifications = classification_engine.classify_fields(
            source="sql",
            object_name="Orders",
            fields=columns,
            field_name_key="name",
            field_type_key="type",
        )

        for col in columns:
            col_class = col_classifications.get(col["name"])
            assets.append(SourceAsset(
                name=col["name"],
                qualified_name=self._qualified_name(db_name, "dbo", "Orders", col["name"]),
                entity_type="custom_sql_column",
                attributes={
                    "dataType": col["type"],
                    "isNullable": col["nullable"],
                    "isPrimaryKey": col["pk"],
                },
                classifications=[col_class] if col_class else [],
            ))

        logger.info(f"Discovered {len(assets)} assets")
        return assets

    def discover_lineage(self) -> list[LineageRelationship]:
        """Discover lineage relationships from the source system.

        In production, this might parse ETL job configs, stored procedures,
        or query logs to determine data flow.
        """
        logger.info("Discovering lineage relationships...")

        db_name = "SalesDB"
        relationships = [
            LineageRelationship(
                process_name="Daily Order Aggregation",
                process_qualified_name=self._qualified_name(db_name, "processes", "daily_order_agg"),
                process_type="StoredProcedure",
                input_qualified_names=[
                    self._qualified_name(db_name, "dbo", "Orders"),
                    self._qualified_name(db_name, "dbo", "Customers"),
                    self._qualified_name(db_name, "dbo", "Products"),
                ],
                output_qualified_names=[
                    self._qualified_name(db_name, "analytics", "OrderSummary"),
                ],
                query_text="EXEC analytics.sp_DailyOrderAggregation",
            ),
        ]

        logger.info(f"Discovered {len(relationships)} lineage relationships")
        return relationships


# =============================================================================
# 8. MAIN ORCHESTRATOR
# =============================================================================

def main():
    """Main entry point — orchestrates the full connector workflow."""

    logger.info("=" * 70)
    logger.info("Microsoft Purview Custom Connector - Example Run")
    logger.info("=" * 70)

    # --- Configuration ---
    config = PurviewConfig.from_environment()
    if not config.account_name:
        config.account_name = "my-purview-account"  # Default for dry run
        logger.warning("No PURVIEW_ACCOUNT_NAME set — running in DRY RUN mode")

    # --- Step 1: Authenticate ---
    logger.info("\n--- Step 1: Authentication ---")
    auth_service = AuthService(config)
    client = auth_service.get_purview_client()

    # --- Step 2: Register custom type definitions ---
    logger.info("\n--- Step 2: Register Type Definitions ---")
    typedef_service = TypeDefService(client)
    typedef_service.register_types()

    # --- Step 3: Discover and create entities ---
    logger.info("\n--- Step 3: Discover and Create Entities ---")
    connector = SQLServerConnector(server_name="sql-prod-01.company.com", config=config)
    assets = connector.discover_assets()

    entity_service = EntityService(client, config)
    entity_service.create_entities_bulk(assets)

    # --- Step 4: Create lineage ---
    logger.info("\n--- Step 4: Create Lineage ---")
    lineage_service = LineageService(client, config)
    lineage_relationships = connector.discover_lineage()
    for rel in lineage_relationships:
        lineage_service.create_lineage(rel)

    # --- Step 5: Apply business metadata ---
    logger.info("\n--- Step 5: Apply Business Metadata ---")
    metadata_service = MetadataService(config)

    # In real usage, you'd use the GUID returned from entity creation
    sample_guid = "00000000-0000-0000-0000-000000000001"
    metadata_service.apply_business_metadata(
        entity_guid=sample_guid,
        metadata={
            "DataQuality": {
                "lastValidated": "2026-02-10",
                "qualityScore": 95,
                "dataOwner": "analytics-team@company.com",
            }
        },
    )

    metadata_service.apply_classifications(
        entity_guid=sample_guid,
        classification_names=["Confidential"],
    )

    # --- Summary ---
    logger.info("\n" + "=" * 70)
    logger.info("Connector run complete!")
    logger.info(f"  Entities created/updated: {len(assets)}")
    logger.info(f"  Lineage relationships:    {len(lineage_relationships)}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
