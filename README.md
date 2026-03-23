# AP Audit Agent

Controller-grade AP audit CLI for comparing transaction exports against ClickUp AP tasks, producing daily Slack warnings, weekly review outputs, schema-gap reporting, and optional low-risk ClickUp updates.

## Easiest Weekly Workflow

Preferred production workflow:

1. Open the AP upload inbox web service.
2. Upload the newest bank-export CSV.
3. The daily and weekly Render jobs fetch `latest.csv` from the inbox automatically.

Local/manual fallback:

1. Drop this week's files into [data](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/data):
   - `transactions.csv`
   - `clickup.csv` or `clickup_tasks.csv`
   - optional `rules.json`
2. Run:

```bash
cd /Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal
python3 ap_audit.py
```

If multiple matching files exist in `data/`, the CLI uses the newest one for transactions and the newest one for ClickUp.

## Daily And Weekly Modes

Weekly review:

```bash
python3 ap_audit.py --mode weekly --lookback-days 7
```

Daily warning run:

```bash
python3 ap_audit.py --mode daily --lookback-days 7 --slack-payload-out /tmp/ap_slack_payload.json
```

Optional low-risk automation:

```bash
export CLICKUP_API_TOKEN='...'
export CLICKUP_LIST_ID='...'
export SLACK_WEBHOOK_URL='...'

python3 ap_audit.py \
  --mode daily \
  --transactions data/transactions.csv \
  --apply-clickup-updates \
  --post-slack
```

## AP Upload Inbox

The repo includes [ap_upload_inbox.py](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/ap_upload_inbox.py), a small internal upload service for weekly bank exports.

Run locally:

```bash
export AP_UPLOAD_TOKEN='machine-download-token'
export AP_ADMIN_USERNAME='apadmin'
export AP_ADMIN_PASSWORD='strong-password'
export AP_SESSION_SECRET='long-random-session-secret'
python3 ap_upload_inbox.py
```

Then open `http://localhost:10000`, sign in, upload the latest CSV, and use:

```bash
export AP_TRANSACTIONS_URL='http://localhost:10000/latest.csv'
export AP_TRANSACTIONS_AUTH_TOKEN='machine-download-token'
```

The service stores:

- the current file as `latest.csv`
- upload metadata as `latest.json`
- archived timestamped copies in `archive/`

The inbox page also analyzes the uploaded transaction history and surfaces:

- urgent AP items when live ClickUp auth is configured
- new charges / unrecognized activity
- vendor spend growth against the previous uploaded file
- savings opportunities / aggressive cut candidates

On Render, the upload inbox uses a persistent disk and the cron services fetch from:

```text
https://anata-ops-ap-inbox.onrender.com/latest.csv
```

Recommended Render env wiring:

- web service `anata-ops-ap-inbox`
  - `AP_UPLOAD_TOKEN`
  - `AP_ADMIN_USERNAME`
  - `AP_ADMIN_PASSWORD`
  - `AP_SESSION_SECRET`
  - `CLICKUP_API_TOKEN`
  - `CLICKUP_LIST_ID`
  - `AP_UPLOAD_STORAGE_DIR=/var/data/ap_upload_inbox`
- cron services `anata-ops-ap-daily` and `anata-ops-ap-weekly`
  - `AP_TRANSACTIONS_URL=https://anata-ops-ap-inbox.onrender.com/latest.csv`
  - `AP_TRANSACTIONS_AUTH_TOKEN=<same value as AP_UPLOAD_TOKEN>`
  - `CLICKUP_API_TOKEN`
  - `CLICKUP_LIST_ID`
  - `SLACK_WEBHOOK_URL`

## Live ClickUp API Workflow

If you want the audit to pull the AP dashboard directly from ClickUp instead of using a CSV export:

```bash
cd /Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal

export CLICKUP_API_TOKEN='your_token_here'
export CLICKUP_LIST_ID='your_ap_list_id'

python3 ap_audit.py \
  --transactions "data/ExportedTransactions (03.23.26).csv" \
  --as-of-date 2026-03-23 \
  --schema-report-out /tmp/ap_schema_report.json
```

If your AP dashboard is easier to target as a View instead of a List, use `CLICKUP_VIEW_ID` instead of `CLICKUP_LIST_ID`.

Supported auth/config sources:

- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID`
- `CLICKUP_VIEW_ID`
- `AP_TRANSACTIONS_URL`
- `AP_TRANSACTIONS_AUTH_TOKEN`
- or equivalent flags: `--clickup-token`, `--clickup-list-id`, `--clickup-view-id`

## Usage

```bash
python3 ap_audit.py \
  --transactions samples/transactions.csv \
  --clickup samples/clickup_tasks.csv \
  --rules samples/rules.json \
  --as-of-date 2026-03-23
```

Optional payload file:

```bash
python3 ap_audit.py \
  --transactions /absolute/path/to/transactions.csv \
  --clickup /absolute/path/to/clickup_ap.csv \
  --payload-out /tmp/ap_payload.json \
  --report-out /tmp/ap_report.txt
```

You can also point the drop-folder workflow at another directory:

```bash
python3 ap_audit.py --data-dir /absolute/path/to/weekly_drop
```

## Supported Inputs

- `CSV`
- `TSV`
- `JSON` arrays or wrapper objects
- simple raw-text blocks with `key: value` lines

## Expected Transaction Fields

- `date`
- `vendor`
- `amount`
- `reference`
- `account`
- `memo`

## Expected ClickUp Fields

- `task_name`
- `vendor_name`
- `amount_due`
- `amount_paid`
- `remaining_balance`
- `frequency`
- `due_date`
- `expected_charge_date`
- `status`
- `payment_method`
- `grouped_flag`
- `notes`
- `transaction_references`
- `cashflow_priority`
- `last_reviewed_date`

## Output

The CLI prints:

1. the required human-readable audit
2. a `MACHINE_ACTION_PAYLOAD` JSON object with:
   - `create_tasks`
   - `update_tasks`
   - `grouped_rollups`
   - `overdue_reviews`
   - `slack_warnings`
   - `exceptions`
   - `weekly_summary`
   - `leadership_summary`
   - `bookkeeper_action_queue`
   - `schema_summary`
   - `clickup_update_actions`
   - `slack_payload`

## Rules File

Use a JSON file to extend vendor aliases, grouping, and recurring logic. See [samples/rules.json](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/samples/rules.json).

Default controller config lives in:

- [config/ap_rules.json](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/config/ap_rules.json)
- [config/ap_automation_config.json](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/config/ap_automation_config.json)
- [config/clickup_ap_schema.json](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/config/clickup_ap_schema.json)

## ClickUp Schema

The CLI can inspect the current ClickUp list custom fields and report the gap against the required AP schema. It does not create missing custom fields automatically; use the schema report plus [config/clickup_ap_schema.json](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/config/clickup_ap_schema.json) as the source of truth when updating the AP list.

## Security

Do not store live ClickUp or Slack secrets in repo-tracked files. Set:

- `CLICKUP_API_TOKEN`
- `CLICKUP_LIST_ID` or `CLICKUP_VIEW_ID`
- `SLACK_WEBHOOK_URL`
- `AP_TRANSACTIONS_URL`
- `AP_TRANSACTIONS_AUTH_TOKEN`
- `AP_UPLOAD_TOKEN`
- `AP_ADMIN_USERNAME`
- `AP_ADMIN_PASSWORD`
- `AP_SESSION_SECRET`

Daily Slack remains intentionally slim:

- overdue items
- due in 1 to 2 days
- partially paid balances still open
- material new obligations only

If a token has been pasted into chat or terminal history, rotate it immediately and replace it with a fresh secret in your secret manager.
