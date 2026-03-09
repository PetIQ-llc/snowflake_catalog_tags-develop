#!/usr/bin/env python3
"""
Module to fetch Snowflake tables from Coalesce Catalog
"""

import logging
from typing import List, Dict, Optional
from .catalog_api_client import CatalogAPIClient

logger = logging.getLogger(__name__)


def fetch_snowflake_tables(client: CatalogAPIClient, warehouse_id: str, limit: int = 1000, page: int = 0) -> Dict:
    """
    Fetch Snowflake tables for a specific warehouse

    Args:
        client: CatalogAPIClient instance
        warehouse_id: Snowflake warehouse ID
        limit: Number of tables to fetch per page
        page: Page number (0-based)

    Returns:
        Dictionary with tables data and metadata
    """
    query = """
    query GetTablesForWarehouse($warehouseId: String!, $limit: Int!, $page: Int!) {
        getTables(
            scope: {
                warehouseId: $warehouseId
            }
            pagination: {
                nbPerPage: $limit
                page: $page
            }
        ) {
            totalCount
            data {
                id
                name
                tableType
                schema {
                    id
                    name
                    database {
                        id
                        name
                        warehouse {
                            id
                            name
                        }
                    }
                }
                tagEntities {
                    tag {
                        id
                        label
                    }
                }
            }
        }
    }
    """

    variables = {
        "warehouseId": warehouse_id,
        "limit": limit,
        "page": page
    }

    try:
        result = client.execute_query(query, variables)
        return result.get("data", {}).get("getTables", {})

    except Exception as e:
        logger.error(f"Failed to fetch tables for warehouse {warehouse_id}: {e}")
        return {}


def get_all_snowflake_tables(client: CatalogAPIClient, warehouse_ids: List[str], limit: Optional[int] = 1000) -> List[Dict]:
    """
    Get all Snowflake tables from the given warehouse IDs

    Args:
        client: CatalogAPIClient instance
        warehouse_ids: List of warehouse IDs
        limit: Maximum number of tables to fetch (None for all tables)

    Returns:
        List of table objects
    """
    if not warehouse_ids:
        logger.warning("No warehouse IDs provided")
        return []

    # For now, use the first warehouse (can be extended to handle multiple)
    warehouse_id = warehouse_ids[0]

    logger.info(f"Fetching tables from warehouse: {warehouse_id}")

    # If no limit specified (--all-tables), fetch all tables using pagination
    if limit is None:
        all_tables = []
        page = 0
        page_size = 1000  # Fetch 1000 at a time for maximum efficiency

        while True:
            tables_data = fetch_snowflake_tables(client, warehouse_id, limit=page_size, page=page)
            tables = tables_data.get("data", [])
            total_count = tables_data.get("totalCount", 0)

            if page == 0:
                logger.info(f"Total tables available: {total_count}")

            all_tables.extend(tables)
            logger.info(f"  Fetched page {page + 1}: {len(tables)} tables (Total so far: {len(all_tables)})")

            # Check if we got all tables or no more data
            if len(tables) < page_size or len(all_tables) >= total_count:
                break

            page += 1

        tables = all_tables
        total_count = len(all_tables)
    else:
        # Fetch with specified limit
        tables_data = fetch_snowflake_tables(client, warehouse_id, limit=limit)
        tables = tables_data.get("data", [])
        total_count = tables_data.get("totalCount", 0)

    logger.info(f"✅ Found {len(tables)} Snowflake tables (Total available: {total_count})")

    if tables:
        logger.info("")
        logger.info("Sample tables found:")
        for i, table in enumerate(tables[:5], 1):
            schema = table.get("schema", {})
            database = schema.get("database", {})
            full_name = f"{database.get('name')}.{schema.get('name')}.{table.get('name')}"
            logger.info(f"  {i}. {full_name} (ID: {table.get('id')})")

        if len(tables) > 5:
            logger.info(f"  ... and {len(tables) - 5} more tables")

    return tables