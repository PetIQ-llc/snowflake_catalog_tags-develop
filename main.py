#!/usr/bin/env python3
"""
Main Orchestrator - Catalog to Snowflake Tag Synchronization

This script orchestrates the complete workflow by importing and using modular components:
1. Fetches Snowflake tables with tags
2. Gets catalog columns for those tables
3. Generates SQL ALTER statements to apply tags in Snowflake
4. Automatically generates DROP TAG statements for removed tags (when previous run exists)

Usage:
    python main.py [options]

Example:
    python main.py --table-id 0012a8fb-644f-466e-bba8-bbc0c6fbc109
    python main.py --all-tables --output-dir ./output
    python main.py --all-tables --no-drops  # Disable automatic DROP generation
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from dotenv import load_dotenv

# Import our modules
from modules import (
    CatalogAPIClient,
    get_snowflake_warehouse_ids,
    get_all_snowflake_tables,
    process_tables_for_columns,
    generate_all_sql_statements,
    create_sql_file_content,
    process_drop_tags,
    save_results
)

# Load environment variables
load_dotenv()


def setup_logging(log_dir: str = "logs", simple_format: bool = False) -> logging.Logger:
    """
    Setup logging configuration

    Args:
        log_dir: Directory for log files
        simple_format: Use simplified format (no module names in console)
    """
    Path(log_dir).mkdir(exist_ok=True)

    log_file = Path(log_dir) / f"catalog_to_snowflake_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # File handler with full details
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    # Console handler with simpler format
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    if simple_format:
        # Simpler format for console - just time and message
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S')
        )
    else:
        # Standard format with module names
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )

    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler]
    )

    return logging.getLogger(__name__)


def main():
    """Main orchestration function"""

    parser = argparse.ArgumentParser(
        description='Synchronize Catalog tags to Snowflake',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process specific table by ID
  python main.py --table-id 0012a8fb-644f-466e-bba8-bbc0c6fbc109

  # Process first 5 tables
  python main.py --limit 5

  # Process ALL tables (no limit)
  python main.py --all-tables

  # Process with custom batch size (even faster for many tables)
  python main.py --all-tables --batch-size 2000

  # Process using sequential mode (original behavior)
  python main.py --limit 20 --no-batch

  # Process without generating DROP TAG statements
  python main.py --all-tables --no-drops

  # Only generate DROP TAG statements (skip regular processing)
  python main.py --drops-only

  # Custom output directory
  python main.py --output-dir ./output
        """
    )

    parser.add_argument('--table-id', type=str, help='Specific table ID to process')
    parser.add_argument('--table-ids', nargs='+', help='Multiple table IDs to process')
    parser.add_argument('--limit', type=int, default=10, help='Limit number of tables to process (default: 10)')
    parser.add_argument('--all-tables', action='store_true', help='Process ALL available tables (no limit)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Number of tables to fetch columns for in each batch (default: 1000)')
    parser.add_argument('--no-batch', action='store_true', help='Disable batch fetching and use sequential processing')
    parser.add_argument('--output-dir', type=str, default='data', help='Output directory for results (default: data)')
    parser.add_argument('--sql-dir', type=str, default='sql', help='Directory for SQL files (default: sql)')
    parser.add_argument('--reports-dir', type=str, default='reports', help='Directory for report files (default: reports)')
    parser.add_argument('--log-dir', type=str, default='logs', help='Directory for log files (default: logs)')
    parser.add_argument('--simple-logs', action='store_true', help='Use simplified console log format')
    parser.add_argument('--no-drops', action='store_true', help='Disable automatic DROP TAG generation')
    parser.add_argument('--drops-only', action='store_true', help='Only generate DROP TAG statements (skip regular processing)')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(args.log_dir, simple_format=args.simple_logs)

    # Get API credentials
    api_token = os.getenv("COALESCE_API_TOKEN")
    api_url = os.getenv("COALESCE_API_URL", "https://api.us.castordoc.com/public/graphql")

    # Clean token
    if api_token:
        api_token = api_token.strip('"').strip("'")

    if not api_token:
        logger.error("API token not found. Please set COALESCE_API_TOKEN environment variable.")
        return 1

    try:
        logger.info("=" * 60)
        logger.info("CATALOG TO SNOWFLAKE TAG SYNCHRONIZATION")
        logger.info("=" * 60)
        logger.info(f"Start time: {datetime.now().isoformat()}")

        # Initialize API client
        client = CatalogAPIClient(api_token, api_url)

        # Step 1: Get Snowflake warehouse IDs
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 1: Fetching Snowflake warehouses")
        logger.info("=" * 60)

        warehouse_ids = get_snowflake_warehouse_ids(client)

        if not warehouse_ids:
            logger.error("No Snowflake warehouses found")
            return 1

        # Step 2: Fetch Snowflake tables
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 2: Fetching Snowflake tables from catalog")
        logger.info("=" * 60)

        limit = None if args.all_tables else args.limit
        snowflake_tables = get_all_snowflake_tables(client, warehouse_ids, limit=limit)

        if not snowflake_tables:
            logger.error("No Snowflake tables found")
            return 1

        # Step 3: Process tables and fetch columns
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 3: Fetching columns for tables")
        logger.info("=" * 60)

        # Log batch mode settings
        if not args.no_batch:
            logger.info(f"Using batch mode with batch size: {args.batch_size}")
        else:
            logger.info("Using sequential mode (batch processing disabled)")

        # Determine which tables to process
        table_ids = None
        process_limit = len(snowflake_tables) if args.all_tables else args.limit

        if args.table_id:
            table_ids = [args.table_id]
            process_limit = 1
        elif args.table_ids:
            table_ids = args.table_ids
            process_limit = len(args.table_ids)

        catalog_columns = process_tables_for_columns(
            client,
            snowflake_tables,
            table_ids=table_ids,
            limit=process_limit,
            batch_size=args.batch_size,
            use_batch=not args.no_batch
        )

        if not catalog_columns:
            logger.warning("No tables with tagged columns found")
            sql_statements = ""
        else:
            # Step 4: Generate SQL statements
            logger.info("")
            logger.info("=" * 60)
            logger.info("STEP 4: Generating SQL statements")
            logger.info("=" * 60)

            sql_statements_list = generate_all_sql_statements(catalog_columns)
            sql_statements = create_sql_file_content(sql_statements_list, catalog_columns)

        # Step 5: Generate DROP TAG statements (default behavior unless disabled)
        drop_sql_content = ""
        drop_count = 0
        if not args.no_drops or args.drops_only:
            logger.info("")
            logger.info("=" * 60)
            logger.info("STEP 5: Generating DROP TAG statements")
            logger.info("=" * 60)

            drop_sql_content, drop_count = process_drop_tags(catalog_columns, args.output_dir)

            if drop_count > 0:
                logger.info(f"Generated {drop_count} DROP TAG statements")
            else:
                logger.info("No tags to drop - all tags from previous run are still present")

        # Step 6: Save results
        logger.info("")
        logger.info("=" * 60)
        logger.info("STEP 6: Saving results")
        logger.info("=" * 60)

        output_files = save_results(
            snowflake_tables,
            catalog_columns,
            sql_statements,
            output_dir=args.output_dir,
            sql_dir=args.sql_dir,
            reports_dir=args.reports_dir,
            drop_sql_content=drop_sql_content
        )

        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Tables found: {len(snowflake_tables)}")
        logger.info(f"Tables with tagged columns: {len(catalog_columns)}")

        if sql_statements:
            sql_lines = sql_statements.split('\n')
            sql_count = len([s for s in sql_lines if s.strip() and not s.startswith('--')])
            logger.info(f"SQL statements generated: {sql_count}")

        if drop_count > 0:
            logger.info(f"DROP TAG statements generated: {drop_count}")

        # Final summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("EXECUTION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"End time: {datetime.now().isoformat()}")
        logger.info("")
        logger.info("Output files:")
        for key, filepath in output_files.items():
            if filepath:
                logger.info(f"  • {key}: {filepath}")

        logger.info("")
        logger.info("✅ Synchronization completed successfully!")

        # Print SQL file locations prominently
        if output_files.get("sql_file"):
            logger.info("")
            logger.info(f"🔧 SQL statements ready to execute: {output_files['sql_file']}")
            logger.info("   Run the SQL file in Snowflake to apply the tags")

        if output_files.get("drop_sql_file"):
            logger.info("")
            logger.info(f"🗑️  DROP TAG statements ready: {output_files['drop_sql_file']}")
            logger.info("   Review carefully before running - these will remove tags from Snowflake")

    except Exception as e:
        logger.error(f"Execution failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    exit(main())