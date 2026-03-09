#!/usr/bin/env python3
"""
Module to generate DROP TAG statements for removed tags
"""

import json
import logging
from typing import Dict, List, Set, Tuple
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def load_previous_run_data(data_dir: str = "data") -> Tuple[Dict, str]:
    """
    Load the most recent previous run's catalog columns data

    Args:
        data_dir: Directory containing previous run files

    Returns:
        Tuple of (catalog_columns dict, filename)
    """
    data_path = Path(data_dir)
    if not data_path.exists():
        logger.warning(f"Data directory {data_dir} does not exist")
        return {}, ""

    # Find all catalog_columns files
    column_files = list(data_path.glob("catalog_columns_*.json"))

    if not column_files:
        logger.info("No previous catalog_columns files found")
        return {}, ""

    # Sort by modification time and get the most recent
    column_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    latest_file = column_files[0]

    logger.info(f"Loading previous run data from: {latest_file}")

    try:
        with open(latest_file, 'r') as f:
            data = json.load(f)
        return data, latest_file.name
    except Exception as e:
        logger.error(f"Failed to load previous run data: {e}")
        return {}, ""


def extract_table_column_tags(catalog_columns: Dict) -> Tuple[Dict[str, Set[str]], Dict[Tuple[str, str], Set[str]]]:
    """
    Extract all table and column tags from catalog data

    Args:
        catalog_columns: Dictionary of catalog columns data

    Returns:
        Tuple of (table_tags, column_tags)
        - table_tags: {full_table_name: {tag_keys}}
        - column_tags: {(full_table_name, column_name): {tag_keys}}
    """
    table_tags = {}
    column_tags = {}

    for table_id, table_data in catalog_columns.items():
        table_info = table_data.get("table", {})
        schema = table_info.get("schema", {})
        database = schema.get("database", {})

        # Skip if table metadata is missing (old format data)
        if not database.get('name') or not schema.get('name') or not table_info.get('name'):
            logger.debug(f"Skipping table {table_id} - missing metadata")
            continue

        full_table_name = f"{database.get('name')}.{schema.get('name')}.{table_info.get('name')}"

        # Extract table-level tags
        table_tag_entities = table_info.get("tagEntities", [])
        if table_tag_entities:
            tag_keys = set()
            for tag_entity in table_tag_entities:
                tag_label = tag_entity.get("tag", {}).get("label", "")
                if ":" in tag_label:
                    key = tag_label.split(":", 1)[0].strip().upper()
                else:
                    key = tag_label.strip().upper()
                tag_keys.add(key)
            if tag_keys:
                table_tags[full_table_name] = tag_keys

        # Extract column-level tags (only if we have valid table metadata)
        columns = table_data.get("columns", [])
        for column in columns:
            column_name = column.get("name", "")
            column_tag_entities = column.get("tagEntities", [])

            if column_tag_entities:
                tag_keys = set()
                for tag_entity in column_tag_entities:
                    tag_label = tag_entity.get("tag", {}).get("label", "")
                    if ":" in tag_label:
                        key = tag_label.split(":", 1)[0].strip().upper()
                    else:
                        key = tag_label.strip().upper()
                    tag_keys.add(key)
                if tag_keys:
                    column_tags[(full_table_name, column_name)] = tag_keys

    return table_tags, column_tags


def compare_runs(previous_data: Dict, current_data: Dict) -> Tuple[Dict[str, Set[str]], Dict[Tuple[str, str], Set[str]]]:
    """
    Compare previous and current runs to find removed tags

    Args:
        previous_data: Previous run's catalog columns data
        current_data: Current run's catalog columns data

    Returns:
        Tuple of (removed_table_tags, removed_column_tags)
    """
    # Extract tags from both runs
    prev_table_tags, prev_column_tags = extract_table_column_tags(previous_data)
    curr_table_tags, curr_column_tags = extract_table_column_tags(current_data)

    # Find removed table tags
    removed_table_tags = {}
    for table, prev_tags in prev_table_tags.items():
        curr_tags = curr_table_tags.get(table, set())
        removed_tags = prev_tags - curr_tags
        if removed_tags:
            removed_table_tags[table] = removed_tags

    # Find tables that no longer exist
    removed_tables = set(prev_table_tags.keys()) - set(curr_table_tags.keys())
    for table in removed_tables:
        removed_table_tags[table] = prev_table_tags[table]

    # Find removed column tags
    removed_column_tags = {}
    for (table, column), prev_tags in prev_column_tags.items():
        curr_tags = curr_column_tags.get((table, column), set())
        removed_tags = prev_tags - curr_tags
        if removed_tags:
            removed_column_tags[(table, column)] = removed_tags

    # Find columns that no longer exist
    removed_columns = set(prev_column_tags.keys()) - set(curr_column_tags.keys())
    for column_key in removed_columns:
        removed_column_tags[column_key] = prev_column_tags[column_key]

    return removed_table_tags, removed_column_tags


def generate_drop_tag_statements(removed_table_tags: Dict[str, Set[str]],
                                 removed_column_tags: Dict[Tuple[str, str], Set[str]]) -> List[str]:
    """
    Generate DROP TAG SQL statements for removed tags

    Args:
        removed_table_tags: Dictionary of tables with removed tags
        removed_column_tags: Dictionary of columns with removed tags

    Returns:
        List of SQL DROP TAG statements
    """
    statements = []

    if not removed_table_tags and not removed_column_tags:
        logger.info("No tags to drop - all tags from previous run are still present")
        return statements

    statements.append("-- ============================================================")
    statements.append("-- DROP TAG statements for removed tags")
    statements.append("-- These tags were present in the previous run but are no longer found")
    statements.append("-- ============================================================")
    statements.append("")

    # Generate DROP statements for table tags
    if removed_table_tags:
        statements.append("-- Table-level tags to remove")
        statements.append(f"-- Tables affected: {len(removed_table_tags)}")
        statements.append("")

        for table, tags in sorted(removed_table_tags.items()):
            statements.append(f"-- Table: {table}")
            for tag in sorted(tags):
                tag_name = tag.replace(" ", "_").replace("-", "_").upper()
                statements.append(f"ALTER TABLE {table}")
                statements.append(f"    UNSET TAG {tag_name};")
            statements.append("")

    # Generate DROP statements for column tags
    if removed_column_tags:
        statements.append("-- Column-level tags to remove")
        statements.append(f"-- Columns affected: {len(removed_column_tags)}")
        statements.append("")

        # Group by table for better organization
        columns_by_table = {}
        for (table, column), tags in removed_column_tags.items():
            if table not in columns_by_table:
                columns_by_table[table] = []
            columns_by_table[table].append((column, tags))

        for table in sorted(columns_by_table.keys()):
            statements.append(f"-- Table: {table}")
            for column, tags in sorted(columns_by_table[table]):
                for tag in sorted(tags):
                    tag_name = tag.replace(" ", "_").replace("-", "_").upper()
                    statements.append(f"-- Column {column}")
                    statements.append(f"ALTER TABLE {table}")
                    statements.append(f"    ALTER COLUMN {column}")
                    statements.append(f"        UNSET TAG {tag_name};")
            statements.append("")

    return statements


def create_drop_tags_sql_file(statements: List[str], previous_filename: str) -> str:
    """
    Create complete DROP TAGS SQL file content with header and footer

    Args:
        statements: List of SQL DROP TAG statements
        previous_filename: Name of the previous run file used for comparison

    Returns:
        Complete SQL file content as string
    """
    # Count statements (count UNSET TAG lines since those are the actual operations)
    drop_count = len([s for s in statements if "UNSET TAG" in s])

    # Build header
    header = [
        "-- ============================================================",
        "-- Snowflake DROP TAG Management Script",
        "-- Generated from Catalog Metadata Comparison",
        f"-- Generated: {datetime.now().isoformat()}",
        f"-- Compared with: {previous_filename}",
        "-- ============================================================",
        "",
        "-- This script will:",
        "-- 1. Remove tags that were present in the previous run",
        "-- 2. But are no longer found in the current catalog",
        "",
        "-- IMPORTANT: Review these changes carefully before executing",
        "-- These tags will be permanently removed from Snowflake objects",
        "",
        "-- ============================================================",
        ""
    ]

    # Build footer
    footer = [
        "",
        "-- ============================================================",
        "-- Summary:",
        f"--   DROP TAG statements: {drop_count}",
        f"--   Previous run file: {previous_filename}",
        "-- ============================================================",
        "-- End of DROP TAG script"
    ]

    # Combine all parts
    if statements:
        full_content = header + statements + footer
    else:
        # No drops needed
        no_drops = [
            "-- No tags to drop",
            "-- All tags from the previous run are still present in the current run"
        ]
        full_content = header + no_drops + footer

    return "\n".join(full_content)


def process_drop_tags(current_catalog_columns: Dict, data_dir: str = "data") -> Tuple[str, int]:
    """
    Main function to process and generate DROP TAG statements

    Args:
        current_catalog_columns: Current run's catalog columns data
        data_dir: Directory containing previous run files

    Returns:
        Tuple of (drop_sql_content, number_of_drops)
    """
    # Load previous run data
    previous_file_data, previous_filename = load_previous_run_data(data_dir)

    if not previous_file_data:
        logger.info("No previous run data found - skipping DROP TAG generation")
        return "", 0

    # Extract the catalog_columns from the saved structure
    if "catalog_columns" in previous_file_data:
        previous_data = previous_file_data["catalog_columns"]
    elif "columns_by_table" in previous_file_data:
        # Old format doesn't have table metadata - can't generate proper DROP statements
        logger.warning("Previous run data is in old format without table metadata")
        logger.warning("Cannot generate DROP TAG statements - table paths would be invalid (None.None.None)")
        logger.info("Please run a full sync first to update the data format")
        return "", 0
    else:
        # Assume it's already in the right format
        previous_data = previous_file_data

    # Compare runs to find removed tags
    removed_table_tags, removed_column_tags = compare_runs(previous_data, current_catalog_columns)

    total_removed = len(removed_table_tags) + len(removed_column_tags)

    if total_removed > 0:
        logger.info(f"Found tags to remove:")
        logger.info(f"  - Tables with removed tags: {len(removed_table_tags)}")
        logger.info(f"  - Columns with removed tags: {len(removed_column_tags)}")

    # Generate DROP statements
    drop_statements = generate_drop_tag_statements(removed_table_tags, removed_column_tags)

    # Create SQL file content
    sql_content = create_drop_tags_sql_file(drop_statements, previous_filename)

    # Count actual DROP statements (count UNSET TAG lines since those are the actual operations)
    drop_count = len([s for s in drop_statements if "UNSET TAG" in s])

    return sql_content, drop_count