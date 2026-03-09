# Catalog to Snowflake Tag Synchronization

A comprehensive solution for synchronizing data catalog tags from Coalesce (Castordoc) to Snowflake tables and columns.

## What This Tool Does

This tool bridges the gap between your data catalog (Coalesce/Castordoc) and Snowflake by:
- **Reading** tags applied to tables and columns in your data catalog
- **Generating** SQL ALTER statements to apply these same tags in Snowflake
- **Enabling** data governance and compliance by ensuring tags are consistent across systems
- **Automating** what would otherwise be a manual, error-prone process

## Project Overview

This project automates the process of:
1. Fetching table and column metadata from Coalesce Catalog API
2. Identifying tables and columns with tags (including PII and sensitive data)
3. Generating Snowflake ALTER statements to apply these tags
4. Providing a complete workflow for tag synchronization

## Architecture

The project uses a **modular architecture** following software engineering best practices:

- **`modules/`** - Contains single-responsibility components:
  - `catalog_api_client.py` - API communication layer
  - `get_warehouses.py` - Warehouse fetching logic
  - `get_tables.py` - Table fetching logic
  - `get_columns.py` - Column fetching and processing
  - `generate_sql.py` - SQL generation logic
  - `save_outputs.py` - Output file management

- **`main.py`** - Orchestrates the modules for the complete workflow

This modular design provides:
- **Maintainability**: Each component has a single responsibility
- **Testability**: Components can be tested independently
- **Reusability**: Modules can be imported and used in other scripts
- **Clarity**: Easy to understand and modify individual components

## Complete Setup Guide (From Scratch)

### Prerequisites Installation

#### 1. Install Visual Studio Code

1. **Download VSCode**:
   - Go to [https://code.visualstudio.com/](https://code.visualstudio.com/)
   - Click "Download for [Your OS]" (Windows/Mac/Linux)
   - Run the installer and follow the prompts

2. **Install Python Extension in VSCode**:
   - Open VSCode
   - Press `Cmd+Shift+X` (Mac) or `Ctrl+Shift+X` (Windows) to open Extensions
   - Search for "Python" by Microsoft
   - Click Install on the Python extension

#### 2. Install Python

1. **Check if Python is installed**:
   - Open Terminal (Mac/Linux) or Command Prompt (Windows)
   - Run: `python3 --version` or `python --version`
   - If Python 3.7+ is installed, skip to step 3

2. **Install Python** (if not installed):
   - **Mac**:
     ```bash
     # Install Homebrew first (if not installed)
     /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

     # Install Python
     brew install python3
     ```
   - **Windows**: Download from [python.org](https://www.python.org/downloads/)
     - **IMPORTANT**: Check "Add Python to PATH" during installation
   - **Linux**:
     ```bash
     sudo apt update
     sudo apt install python3 python3-pip
     ```

### Project Setup in VSCode

#### 1. Open the Project

1. **Clone or Download the Project**:
   - If using Git: `git clone [repository-url]`
   - Or download and extract the ZIP file

2. **Open in VSCode**:
   - Open VSCode
   - Click `File > Open Folder...`
   - Navigate to and select the `CatalogTags` folder
   - Click Open

#### 2. Set Up Python Environment

1. **Open VSCode Terminal**:
   - Press `` Ctrl+` `` (backtick) or `View > Terminal`

2. **Create Virtual Environment** (recommended):
   ```bash
   # In the VSCode terminal, run:
   python3 -m venv venv

   # Activate it:
   # Mac/Linux:
   source venv/bin/activate

   # Windows:
   venv\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   # Option 1: Install from requirements.txt (recommended)
   pip install -r requirements.txt

   # Option 2: Install manually
   pip install requests python-dotenv
   ```

#### 3. Configure API Credentials

1. **Create .env file**:
   - In VSCode, right-click in the file explorer
   - Select "New File"
   - Name it `.env`
   - Add your credentials:
   ```env
   # Coalesce Catalog API Configuration
   COALESCE_API_TOKEN='your_api_token_here'
   COALESCE_API_URL='https://api.us.castordoc.com/public/graphql'
   ```

2. **Get Your API Token**:
   - Log into your Coalesce/Castordoc account
   - Navigate to Settings > API Access
   - Generate or copy your API token
   - Paste it in the `.env` file (replacing `your_api_token_here`)

### Running the Project in VSCode

#### Method 1: Using VSCode Terminal (Recommended)

1. **Open Terminal** in VSCode (`` Ctrl+` ``)

2. **Run the main script**:
   ```bash
   # Process first 10 tables (default)
   python main.py

   # Process a specific table
   python main.py --table-id 0012a8fb-644f-466e-bba8-bbc0c6fbc109

   # Process ALL tables (no limit)
   python main.py --all-tables

   # With cleaner output
   python main.py --simple-logs
   ```

3. **Check the output**:
   - Watch the terminal for progress
   - Generated SQL files will be in the `sql/` folder
   - Reports will be in the `reports/` folder
   - Logs will be in the `logs/` folder

#### Method 2: Using VSCode Run and Debug

1. **Create launch configuration**:
   - Press `F5` or click Run > Start Debugging
   - Select "Python" as the environment
   - Select "Python File" as the configuration

2. **Or create a launch.json**:
   - Click Run > Add Configuration
   - Add this configuration:
   ```json
   {
     "version": "0.2.0",
     "configurations": [
       {
         "name": "Run Main",
         "type": "python",
         "request": "launch",
         "program": "${workspaceFolder}/main.py",
         "args": ["--limit", "5", "--simple-logs"],
         "console": "integratedTerminal"
       }
     ]
   }
   ```

### VSCode Tips for This Project

1. **View Output Files**:
   - Click on the `sql/` folder in the explorer to see generated SQL files
   - Double-click any `.sql` file to view it
   - VSCode has built-in SQL syntax highlighting

2. **Search in Files**:
   - Press `Cmd+Shift+F` (Mac) or `Ctrl+Shift+F` (Windows) to search across all files

3. **View Logs**:
   - Navigate to the `logs/` folder
   - Click on the latest log file to view detailed execution information

4. **Python Interactive Mode**:
   - Select any Python code
   - Right-click and choose "Run Selection/Line in Python Terminal"

5. **Git Integration** (if using Git):
   - The Source Control tab (`Ctrl+Shift+G`) shows file changes
   - You can commit directly from VSCode

## Folder Structure

```
CatalogTags/
├── main.py                               # Main orchestrator (entry point)
├── .env                                  # Environment variables (API tokens)
├── requirements.txt                      # Python dependencies
├── README.md                            # This documentation
├── modules/                              # Modular components
│   ├── __init__.py                      # Module exports
│   ├── catalog_api_client.py            # Base API client
│   ├── get_warehouses.py                # Fetch warehouse IDs
│   ├── get_tables.py                    # Fetch Snowflake tables with tags
│   ├── get_columns.py                   # Fetch columns with tags
│   ├── generate_sql.py                  # Generate ALTER statements (table & column)
│   └── save_outputs.py                  # Save results to files
├── data/                                 # JSON data files (created on first run)
│   └── [generated JSON files]
├── sql/                                  # SQL output files (created on first run)
│   └── [generated SQL files]
├── reports/                              # Sync reports (created on first run)
│   └── [generated report files]
└── logs/                                 # Execution logs (created on first run)
    └── [generated log files]
```

**Note**: The `data/`, `sql/`, `reports/`, and `logs/` directories are created automatically when you first run the script.

## Quick Setup (If Prerequisites Are Installed)

If you already have VSCode, Python, and Git installed, follow these quick steps:

1. **Clone/Download the project and open in VSCode**
2. **Install dependencies**: `pip install -r requirements.txt`
3. **Create `.env` file with your API token**
4. **Run**: `python main.py`

## Usage

### Using the Main Orchestrator

The `main.py` script orchestrates the complete workflow using modular components:

```bash
# Process first 10 tables (default)
python main.py

# Process a specific table by ID
python main.py --table-id 0012a8fb-644f-466e-bba8-bbc0c6fbc109

# Process first 5 tables
python main.py --limit 5

# Process ALL available tables (no limit)
python main.py --all-tables

# Process multiple specific tables
python main.py --table-ids table-id-1 table-id-2 table-id-3

# Use simplified console output
python main.py --simple-logs

# Custom directories
python main.py --output-dir ./my-data --sql-dir ./my-sql --reports-dir ./my-reports
```

### Using Individual Modules

The modular architecture allows you to import and use individual components in your own scripts:

```python
from modules import CatalogAPIClient, get_snowflake_warehouse_ids, get_all_snowflake_tables

# Initialize client
client = CatalogAPIClient(api_token, api_url)

# Get warehouses
warehouse_ids = get_snowflake_warehouse_ids(client)

# Get tables
tables = get_all_snowflake_tables(client, warehouse_ids, limit=10)
```

## Workflow Steps

### Step 1: Fetch Tables
The script fetches all Snowflake tables from the Coalesce Catalog API, including:
- Table metadata (name, schema, database)
- Table-level tags (process tags, workflow tags, etc.)

### Step 2: Get Column Tags
For each table, the script fetches column information including:
- Column names
- Sensitivity tags (location, name, phone, email, PII, etc.)
- Other classification tags

### Step 3: Generate SQL
The script generates optimized Snowflake ALTER statements that:
- Apply table-level tags using PROCESS_TAG
- Apply column-level tags using SENSITIVITY tag
- Group columns by tag type for efficiency
- Use proper Snowflake syntax
- Include context-setting commands (USE DATABASE, USE SCHEMA)

### Step 4: Execute in Snowflake
Before running the generated SQL:

1. **Create the tags in Snowflake** (if they don't exist):
```sql
-- For column-level sensitivity tags
CREATE TAG IF NOT EXISTS SENSITIVITY COMMENT = 'Sensitivity classification for columns';

-- For table-level process tags
CREATE TAG IF NOT EXISTS PROCESS_TAG COMMENT = 'Process or workflow tags for tables';
```

2. **Run the generated ALTER statements** from the `sql/` folder

## Tag Mapping

The script automatically converts catalog tags to Snowflake-friendly format:

### Column-Level Tags (SENSITIVITY)
| Catalog Tag | Snowflake Tag |
|------------|---------------|
| catalog:sensitive location | SENSITIVE_LOCATION |
| catalog:sensitive name | SENSITIVE_NAME |
| catalog:sensitive phone | SENSITIVE_PHONE |
| catalog:sensitive email | SENSITIVE_EMAIL |
| catalog:sensitive pii | SENSITIVE_PII |

### Table-Level Tags (PROCESS_TAG)
| Catalog Tag | Snowflake Tag |
|------------|---------------|
| catalog:workflow production | WORKFLOW_PRODUCTION |
| catalog:process etl | PROCESS_ETL |
| catalog:process analytics | PROCESS_ANALYTICS |

**Note**: All tags have the `catalog:` prefix removed and spaces replaced with underscores.

## Output Files

### Data Files (data/)
- `snowflake_tables_YYYYMMDD_HHMMSS.json` - All fetched Snowflake tables
- `catalog_columns_YYYYMMDD_HHMMSS.json` - Columns with tags for processed tables

### SQL Files (sql/)
- `snowflake_alter_tags_YYYYMMDD_HHMMSS.sql` - Generated ALTER statements ready to execute

### Report Files (reports/)
- `sync_report_YYYYMMDD_HHMMSS.txt` - Summary report of the sync operation

### Log Files (logs/)
- `catalog_to_snowflake_YYYYMMDD_HHMMSS.log` - Detailed execution logs

## Example Output

Sample ALTER statements generated:

### Table-Level Tag
```sql
-- Table: PROD_DB.ANALYTICS.CUSTOMER_ORDERS
-- Table-level tags: 1

-- Apply table-level tag: catalog:process etl
ALTER TABLE PROD_DB.ANALYTICS.CUSTOMER_ORDERS
    SET TAG PROCESS_TAG = 'PROCESS_ETL';
```

### Column-Level Tags
```sql
-- Column-level tags: 2 columns with tags

ALTER TABLE PROD_DB.ANALYTICS.CUSTOMER_ORDERS ALTER
    COLUMN CUSTOMER_NAME SET TAG SENSITIVITY = 'SENSITIVE_NAME'
    , COLUMN EMAIL_ADDRESS SET TAG SENSITIVITY = 'SENSITIVE_EMAIL'
;
```

## Troubleshooting

### Common Issues

1. **API Token Issues**
   - Ensure your token is in the `.env` file
   - Remove any extra quotes from the token (use single quotes: `'token'`)
   - Token should not have spaces or newlines

2. **API Endpoint**
   - Default is `https://api.us.castordoc.com/public/graphql`
   - The `.env` file overrides the default if specified
   - Verify the correct API URL for your region

3. **GraphQL Errors**
   - Check that your account has proper permissions
   - Verify table IDs exist in your catalog
   - Ensure you have access to the Snowflake warehouse

4. **VSCode Specific Issues**
   - **Python not found**: Select the correct Python interpreter (`Cmd+Shift+P` > "Python: Select Interpreter")
   - **Module not found**: Ensure you've installed dependencies (`pip install requests python-dotenv`)
   - **Permission denied**: On Mac/Linux, you may need to use `python3` instead of `python`
   - **Virtual environment not activated**: Run `source venv/bin/activate` (Mac/Linux) or `venv\Scripts\activate` (Windows)

5. **No Tables Found**
   - Verify you have Snowflake warehouses configured in Coalesce
   - Check that tables have been imported into the catalog
   - Ensure tables have tags applied in the catalog

### Logging

All operations are logged to the `logs/` directory. Check the latest log file for detailed error messages.

## Verifying Your Setup

After installation, verify everything is working:

```bash
# Check Python version
python3 --version  # Should be 3.7 or higher

# Check dependencies are installed
python -c "import requests, dotenv; print('Dependencies OK')"

# Test API connection (processes 1 table)
python main.py --limit 1 --simple-logs
```

If successful, you should see:
- Tables being fetched from the catalog
- Columns being processed
- SQL statements generated in the `sql/` folder
- A summary report in the `reports/` folder

## Advanced Options

### Command Line Arguments

| Argument | Description | Example |
|----------|-------------|---------|
| `--table-id` | Process a specific table | `--table-id abc123` |
| `--table-ids` | Process multiple tables | `--table-ids id1 id2 id3` |
| `--limit` | Number of tables to process | `--limit 5` |
| `--all-tables` | Process ALL tables (no limit) | `--all-tables` |
| `--output-dir` | Directory for JSON data | `--output-dir ./my-data` |
| `--sql-dir` | Directory for SQL files | `--sql-dir ./my-sql` |
| `--reports-dir` | Directory for reports | `--reports-dir ./my-reports` |
| `--log-dir` | Directory for log files | `--log-dir ./my-logs` |
| `--simple-logs` | Cleaner console output | `--simple-logs` |

## Requirements

### Software Requirements
- **Python**: Version 3.7 or higher (3.8+ recommended)
- **Visual Studio Code**: Latest version
- **Python Libraries**:
  - `requests`: For API communication
  - `python-dotenv`: For environment variable management

### Access Requirements
- **Coalesce/Castordoc Account**: With API access enabled
- **API Token**: Generated from Coalesce settings
- **Snowflake Account**: With permissions to:
  - Create tags (ACCOUNTADMIN or similar role)
  - Alter tables and columns
  - Access to target database and schemas

### System Requirements
- **Operating System**: Windows 10+, macOS 10.14+, or Linux
- **Memory**: 4GB RAM minimum
- **Storage**: 100MB free space for outputs

## License

This project is for internal use. Please ensure compliance with your organization's data governance policies.

## Typical End-to-End Workflow

1. **Setup (One-time)**:
   - Install VSCode and Python
   - Clone/download this project
   - Install dependencies: `pip install -r requirements.txt`
   - Add your API token to `.env` file

2. **Tag Management in Catalog**:
   - Apply tags to tables and columns in Coalesce/Castordoc UI
   - Tags should follow naming convention: `catalog:category subcategory`

3. **Generate SQL**:
   ```bash
   # Run from VSCode terminal
   python main.py --all-tables --simple-logs
   ```

4. **Execute in Snowflake**:
   - Open Snowflake worksheet
   - Create tags if needed:
     ```sql
     CREATE TAG IF NOT EXISTS SENSITIVITY;
     CREATE TAG IF NOT EXISTS PROCESS_TAG;
     ```
   - Run the generated SQL from `sql/snowflake_alter_tags_*.sql`

5. **Verify**:
   - Check Snowflake to confirm tags are applied
   - Review the report in `reports/sync_report_*.txt`

## Support

For issues or questions, please check the logs in the `logs/` directory for detailed error messages.