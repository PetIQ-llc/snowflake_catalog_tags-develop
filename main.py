#!/usr/bin/env python3

"""
Main Orchestrator - Catalog to Snowflake Tag Synchronization

This script orchestrates the complete workflow:
1. Fetch Snowflake tables from the catalog
2. Retrieve column metadata and tags
3. Generate SQL ALTER statements to apply tags in Snowflake
4. Generate DROP TAG statements if tags were removed since the last run
"""

# -----------------------------------------------------------
# Standard Python libraries used for system operations
# -----------------------------------------------------------
import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List

# -----------------------------------------------------------
# Loads environment variables from a .env file
# Useful for local development
# -----------------------------------------------------------
from dotenv import load_dotenv

# -----------------------------------------------------------
# Import custom modules that contain the actual logic
# These modules handle API calls, SQL generation, and file saving
# -----------------------------------------------------------
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

# -----------------------------------------------------------
# Load environment variables from .env file (if present)
# -----------------------------------------------------------
load_dotenv()


# -----------------------------------------------------------
# Configure logging for both console output and log files
# -----------------------------------------------------------
def setup_logging(log_dir: str = "logs", simple_format: bool = False) -> logging.Logger:
    """
    Setup logging configuration

    Creates:
    • A log file with full detail
    • Console output for real-time monitoring
    """

    # Ensure the log directory exists
    Path(log_dir).mkdir(exist_ok=True)

    # Create a timestamped log file
    log_file = Path(log_dir) / f"catalog_to_snowflake_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # -----------------------------------------
    # File logger (full details for debugging)
    # -----------------------------------------
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )

    # -----------------------------------------
    # Console logger (simpler output for CLI)
    # -----------------------------------------
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    if simple_format:
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(message)s', datefmt='%H:%M:%S')
        )
    else:
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )

    # -----------------------------------------
    # Configure the root logger
    # -----------------------------------------
    logging.basicConfig(
        level=logging.INFO,
        handlers=[file_handler, console_handler]
    )

    return logging.getLogger(__name__)


# -----------------------------------------------------------
# Main function that orchestrates the full workflow
# -----------------------------------------------------------
def main():

    # -----------------------------------------
    # Define CLI arguments the script accepts
    # -----------------------------------------
    parser = argparse.ArgumentParser(
        description='Synchronize Catalog tags to Snowflake',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --table-id <id>
  python main.py --limit 5
  python main.py --all-tables
        """
    )

    # -------------------------------------------------------
    # CLI options that control which tables are processed
    # -------------------------------------------------------
    parser.add_argument('--table-id', type=str, help='Specific table ID to process')
    parser.add_argument('--table-ids', nargs='+', help='Multiple table IDs to process')
    parser.add_argument('--limit', type=int, default=10, help='Limit number of tables')
    parser.add_argument('--all-tables', action='store_true', help='Process all tables')

    # -------------------------------------------------------
    # Batch processing settings for API calls
    # -------------------------------------------------------
    parser.add_argument('--batch-size', type=int, default=1000)
    parser.add_argument('--no-batch', action='store_true')

    # -------------------------------------------------------
    # Output directories for generated files
    # -------------------------------------------------------
    parser.add_argument('--output-dir', type=str, default='data')
    parser.add_argument('--sql-dir', type=str, default='sql')
    parser.add_argument('--reports-dir', type=str, default='reports')
    parser.add_argument('--log-dir', type=str, default='logs')

    # -------------------------------------------------------
    # Logging options
    # -------------------------------------------------------
    parser.add_argument('--simple-logs', action='store_true')

    # -------------------------------------------------------
    # Tag deletion options
    # -------------------------------------------------------
    parser.add_argument('--no-drops', action='store_true')
    parser.add_argument('--drops-only', action='store_true')

    # Parse command line arguments
    args = parser.parse_args()

    # -------------------------------------------------------
    # Initialize logging
    # -------------------------------------------------------
    logger = setup_logging(args.log_dir, simple_format=args.simple_logs)

    # -------------------------------------------------------
    # Load API credentials from environment variables
    # -------------------------------------------------------
    api_token = os.getenv("COALESCE_API_TOKEN")
    api_url = os.getenv("COALESCE_API_URL", "https://api.us.castordoc.com/public/graphql")

    # Clean quotes if token contains them
    if api_token:
        api_token = api_token.strip('"').strip("'")

    # Stop execution if API token is missing
    if not api_token:
        logger.error("API token not found. Please set COALESCE_API_TOKEN.")
        return 1

    try:

        # -------------------------------------------------------
        # Print script start header
        # -------------------------------------------------------
        logger.info("=" * 60)
        logger.info("CATALOG TO SNOWFLAKE TAG SYNCHRONIZATION")
        logger.info("=" * 60)
        logger.info(f"Start time: {datetime.now().isoformat()}")

        # -------------------------------------------------------
        # Initialize API client for catalog requests
        # -------------------------------------------------------
        client = CatalogAPIClient(api_token, api_url)

        # -------------------------------------------------------
        # STEP 1: Get Snowflake warehouse IDs
        # -------------------------------------------------------
        logger.info("STEP 1: Fetching Snowflake warehouses")

        warehouse_ids = get_snowflake_warehouse_ids(client)

        if not warehouse_ids:
            logger.error("No Snowflake warehouses found")
            return 1

        # -------------------------------------------------------
        # STEP 2: Fetch Snowflake tables from catalog
        # -------------------------------------------------------
        logger.info("STEP 2: Fetching Snowflake tables")

        limit = None if args.all_tables else args.limit
        snowflake_tables = get_all_snowflake_tables(client, warehouse_ids, limit=limit)

        if not snowflake_tables:
            logger.error("No Snowflake tables found")
            return 1

        # -------------------------------------------------------
        # STEP 3: Fetch column metadata for tables
        # -------------------------------------------------------
        logger.info("STEP 3: Fetching columns for tables")

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

        # -------------------------------------------------------
        # STEP 4: Generate SQL ALTER statements for tags
        # -------------------------------------------------------
        if catalog_columns:

            logger.info("STEP 4: Generating SQL statements")

            sql_statements_list = generate_all_sql_statements(catalog_columns)

            sql_statements = create_sql_file_content(
                sql_statements_list,
                catalog_columns
            )

        else:
            logger.warning("No tagged columns found")
            sql_statements = ""

        # -------------------------------------------------------
        # STEP 5: Generate DROP TAG statements if needed
        # -------------------------------------------------------
        drop_sql_content = ""
        drop_count = 0

        if not args.no_drops or args.drops_only:

            logger.info("STEP 5: Generating DROP TAG statements")

            drop_sql_content, drop_count = process_drop_tags(
                catalog_columns,
                args.output_dir
            )

        # -------------------------------------------------------
        # STEP 6: Save output files (SQL, reports, data snapshots)
        # -------------------------------------------------------
        logger.info("STEP 6: Saving results")

        output_files = save_results(
            snowflake_tables,
            catalog_columns,
            sql_statements,
            output_dir=args.output_dir,
            sql_dir=args.sql_dir,
            reports_dir=args.reports_dir,
            drop_sql_content=drop_sql_content
        )

        # -------------------------------------------------------
        # Final summary printed to logs
        # -------------------------------------------------------
        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)

        logger.info(f"Tables found: {len(snowflake_tables)}")
        logger.info(f"Tables with tagged columns: {len(catalog_columns)}")

        if drop_count > 0:
            logger.info(f"DROP TAG statements generated: {drop_count}")

        logger.info("Synchronization completed successfully")

    # -------------------------------------------------------
    # Error handling block
    # -------------------------------------------------------
    except Exception as e:

        logger.error(f"Execution failed: {e}")

        import traceback
        logger.error(traceback.format_exc())

        return 1

    return 0


# -----------------------------------------------------------
# Script entry point
# This runs the main() function when executed
# -----------------------------------------------------------
if __name__ == "__main__":
    exit(main())
