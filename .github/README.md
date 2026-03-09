# GitHub Actions Setup for Catalog Tag Synchronization

This repository includes GitHub Actions workflows to automate the synchronization of tags from the Coalesce Catalog to Snowflake.

## Available Workflows

### 1. Sync Catalog Tags (`sync-catalog.yml`)
**One workflow for everything:**
- **Runs automatically** every day at 2 AM UTC
- **Can be triggered manually** with custom options
- Saves results as downloadable artifacts
- Maintains history for DROP TAG generation
- **Smart mode detection** based on input fields

**Manual trigger modes:**
- `auto` - Default settings (all tables, batch size 1000)
- `full` - Process all tables without default batching
- `drops-only` - Only generate DROP statements

**Auto-detected modes (based on fields):**
- When you enter **specific table IDs** → Automatically runs in **specific mode**
- When you enter a **table limit number** → Automatically runs in **limited mode**
- Priority: table IDs > table limit > selected mode

### 2. Test Configuration (`test-setup.yml`)
- Verifies your GitHub Actions configuration
- Checks dependencies and environment
- Tests without making API calls

## Setup Instructions

### 1. Configure Repository Secrets and Variables

Go to **Settings > Secrets and variables > Actions**

#### Required Secret:
In the **Secrets** tab, add:
- `COALESCE_API_TOKEN` - Your Coalesce API token (keep this as a secret since it's sensitive)

#### Optional Configuration:
In the **Variables** tab (recommended for non-sensitive values):
- `COALESCE_API_URL` - API endpoint
  - Default if not set: `https://api.us.castordoc.com/public/graphql`
  - You can also add this as a Secret if you prefer

In the **Secrets** tab (for sensitive values):
- `SLACK_WEBHOOK_URL` - For Slack notifications (optional)

### 2. Enable GitHub Actions

1. Go to **Actions** tab in your repository
2. If prompted, enable GitHub Actions for the repository

## Using the Workflows

### Daily Automated Sync

The sync runs automatically every day at 2 AM UTC.

### Manual Sync (On-Demand)

To run the sync manually with custom settings:

1. Go to **Actions** tab
2. Select **"Sync Catalog Tags"**
3. Click **"Run workflow"**
4. **Smart Mode Selection**:

   **Just fill in what you need - the workflow auto-detects the mode:**
   - **Want specific tables?** → Enter table IDs (comma-separated)
   - **Want limited tables?** → Enter a number in table limit
   - **Want all tables?** → Leave fields empty, use `auto` mode
   - **Want DROP statements only?** → Select `drops-only` mode

   **The dropdown mode is only used when no fields are filled:**
   - **auto** (default): Process all tables with batch size 1000
   - **full**: Process all tables without default batching
   - **drops-only**: Only generate DROP TAG statements

5. **Optional**: Set custom batch size for API calls
6. Click **"Run workflow"**

**Examples:**
- Enter `10` in table limit → Runs in limited mode with 10 tables
- Enter `abc123,def456` in table IDs → Runs in specific mode with those tables
- Select `drops-only`, leave fields empty → Generates only DROP statements
- Leave everything default → Runs in auto mode (all tables)

### Test Your Setup

Before running the sync, verify everything is configured correctly:

1. Go to **Actions** tab
2. Select **"Test Configuration"**
3. Click **"Run workflow"**
4. Choose your test mode:

#### Test Mode Options:

**`environment`** - Configuration Check (No API calls)
- ✅ Verifies Python version and dependencies
- ✅ Checks if required files exist
- ✅ Tests Python module imports
- ✅ Confirms secrets/variables are configured
- ✅ No API calls made - safe and fast
- **Use this first** to verify basic setup

**`dry-run`** - Full Test with API
- ✅ Everything from `environment` mode
- ✅ Actually calls the API with `--limit 1`
- ✅ Verifies API token is valid
- ✅ Confirms API URL is correct
- ✅ Shows real output from the script
- **Use this** to confirm end-to-end functionality

5. Review the summary for any issues
6. Fix any ❌ items before proceeding

## 📥 Accessing Your Results

### Download from GitHub Actions

1. Go to **Actions** tab
2. Click on a completed workflow run
3. Scroll down to the **Summary** section
4. Find **Artifacts** and download:

#### Available Artifacts:
- **`sql-files-{run-number}`** - Contains:
  - `snowflake_alter_tags_*.sql` - CREATE and SET TAG statements
  - `snowflake_drop_tags_*.sql` - UNSET TAG statements
- **`reports-{run-number}`** - Contains:
  - `sync_report_*.txt` - Detailed synchronization report
- **`catalog-data`** - Previous run data (used internally for DROP TAG comparison)

### Artifact Retention:
- SQL files: 90 days
- Reports: 90 days
- Catalog data: 30 days

## Workflow Features

### Smart Mode Detection
- **Auto-detects mode** based on input fields
- Enter table IDs → runs specific mode
- Enter table limit → runs limited mode
- No need to manually select mode when using these fields

### Continuous Comparison
- Previous run data is preserved as artifacts
- DROP TAG statements are automatically generated by comparing with the last run
- No database or external storage needed
- Gracefully handles missing artifacts (shows warning instead of error)

### GitHub Actions Summary
Each workflow run creates a summary page showing:
- Tables processed
- Tables with tags found
- SQL statements generated
- DROP statements generated
- Links to download artifacts

### Slack Notifications (Optional)
If configured, sends notifications with:
- Completion status
- Key metrics
- Direct link to download artifacts

## Example Workflow

1. **Daily Run**: Automated sync runs at 2 AM
2. **Check Results**:
   - Go to Actions tab in the morning
   - Click on the latest run
   - Review the summary
3. **Download SQL**:
   - Download `sql-files-*` artifact
   - Extract the ZIP file
4. **Execute in Snowflake**:
   - Review the SQL statements
   - Run `snowflake_alter_tags_*.sql` to add/update tags
   - Run `snowflake_drop_tags_*.sql` to remove obsolete tags

## Quick Start Guide

After configuring your secrets/variables:

1. **Test Configuration First**:
   - Go to **Actions** > **Test Configuration**
   - Run with `environment` mode to check setup
   - If all ✅, run with `dry-run` mode to test API
   - Fix any ❌ items shown in the summary

2. **Run Your First Sync**:
   - Go to **Actions** > **Sync Catalog Tags**
   - Enter `10` in the "Number of tables" field (auto-selects limited mode)
   - Download and review the generated SQL

3. **Enable Daily Automation**:
   - Once verified working, the daily sync will run automatically
   - Check Actions tab each morning for results

## Customization

### Changing Schedule

Edit `.github/workflows/sync-catalog.yml`:
```yaml
schedule:
  - cron: '0 2 * * *'  # Daily at 2 AM UTC
```

Common schedules:
- `'0 */6 * * *'` - Every 6 hours
- `'0 9 * * 1'` - Weekly on Monday at 9 AM
- `'0 0 1 * *'` - Monthly on the 1st

### Processing Limits

Default settings optimized for performance:
- Batch size: 1000 tables per API call
- Page size: 10,000 columns per request
- Retention: 90 days for SQL, 30 days for data

## Troubleshooting

### Workflow Not Running
- Check if Actions are enabled in repository settings
- Verify secrets are configured correctly
- Check workflow syntax in Actions tab

### No Artifacts Generated
- Check logs for errors in the sync step
- Verify API credentials are correct
- Check if tables have any tags

### "Artifact not found" Warning
- This is normal for the first run or after artifacts expire
- The workflow handles this gracefully and continues
- Previous catalog data is used for comparison when available

### Can't Download Artifacts
- Artifacts expire after retention period
- Must be logged into GitHub to download
- Check browser download settings

## Best Practices

1. **Test First**: Enter `10` in table limit field to test with 10 tables
2. **Review SQL**: Always review generated SQL before executing
3. **Download Promptly**: Download important artifacts before 90-day expiration
4. **Monitor Runs**: Check workflow summaries weekly
5. **Keep Repository Clean**: All outputs are artifacts, not commits
6. **Use Smart Fields**: Just enter values - mode auto-detects correctly

## Security Considerations

- API tokens are stored as encrypted secrets
- Workflows run in isolated GitHub-hosted runners
- Generated files contain no sensitive data (only metadata)
- Artifacts require GitHub authentication to download
