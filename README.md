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

Authentication is fail-closed by default. Browser access to the AP inbox, `/latest.csv`, `/upload`, `/admin/*`, and `/website-ops/*` requires `AP_ADMIN_USERNAME` and `AP_ADMIN_PASSWORD`; machine downloads of `/latest.csv` require a configured matching `AP_UPLOAD_TOKEN`. For temporary local-only development, set `ANATA_ALLOW_UNAUTHENTICATED_LOCAL=true`.

The service stores:

- the current file as `latest.csv`
- upload metadata as `latest.json`
- archived timestamped copies in `archive/`

The inbox page also analyzes the uploaded transaction history and surfaces:

- urgent AP items when live ClickUp auth is configured
- new charges / unrecognized activity
- vendor spend growth against the previous uploaded file
- savings opportunities / aggressive cut candidates
- connected-system status for ClickUp and QuickBooks vendor sync

Vendor recognition now prefers the richest bank descriptor available, suppresses internal `A2A Transfer` movements, and uses connected ClickUp/QBO vendors to reduce false "new vendor" alerts.

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
  - `QBO_CLIENT_ID`
  - `QBO_CLIENT_SECRET`
  - `QBO_REALM_ID`
  - `QBO_REFRESH_TOKEN`
  - `QBO_TOKEN_STORE_PATH=/var/data/ap_upload_inbox/qbo_tokens.json`
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

## QuickBooks Online Vendor Sync

The inbox can enrich vendor normalization from QuickBooks Online so generic bank descriptors are less likely to show up as fake "new vendors".

Configure on the inbox service:

- `QBO_CLIENT_ID`
- `QBO_CLIENT_SECRET`
- `QBO_REALM_ID`
- `QBO_REFRESH_TOKEN`
- `QBO_TOKEN_STORE_PATH=/var/data/ap_upload_inbox/qbo_tokens.json`

The service refreshes access tokens as needed and persists the latest rotated refresh token to `QBO_TOKEN_STORE_PATH`, which is important because Intuit rotates refresh tokens over time. This currently powers the inbox analysis and vendor matching.

## Website Ops

The repo now also includes an internal website-ops surface for production SEO review and feedback intake:

- report library at `/website-ops/reports/`
- latest report shortcut at `/website-ops/reports/latest`
- structured feedback intake at `/website-ops/feedback`
- open work queue at `/website-ops/queue`
- backup browser at `/website-ops/backups/`

The read-only collection and report pipeline lives in [website_ops/core.py](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/website_ops/core.py). The scheduled runner lives in [scripts/run_website_ops.py](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/scripts/run_website_ops.py) and reads [config/website_ops.json](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/config/website_ops.json).

Run a daily sweep locally:

```bash
python3 scripts/run_website_ops.py --mode daily
```

Override monitored URLs without editing config:

```bash
WEBSITE_OPS_URLS='https://anatainc.com/,https://anatainc.com/services/' \
python3 scripts/run_website_ops.py --mode daily
```

The runner writes JSON, Markdown, and HTML artifacts under `website-ops/reports/<mode>/` and folds in feedback records stored under `website-ops/feedback/`.

Approved dashboard items can now carry an explicit safe action payload. The first supported action is:

- `replace_primary_heading`

When an item is approved with:

- `action_type=replace_primary_heading`
- `action_value=<new H1 text>`
- optional `target_post_id=<WordPress page ID>`

the runner can execute it against WordPress on the next scheduled pass, back up the original page payload, verify the live H1, and mark the feedback record `done` or `error`.

Enable live execution with environment variables:

```bash
export WEBSITE_OPS_EXECUTE_APPROVED=true
export WP_SITE_URL='https://anatainc.com'
export WP_USERNAME='...'
export WP_APPLICATION_PASSWORD='...'
```

Leave `WEBSITE_OPS_EXECUTE_APPROVED` unset to keep the system in report-and-approval mode only.

## Fulfillment Support Environment

The repo now also includes a dedicated environment scaffold for a fulfillment customer service agent that is intended to run in Codex cloud against this workspace rather than on a local machine.

The environment runner lives in [scripts/run_fulfillment_support.py](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/scripts/run_fulfillment_support.py) and reads [config/fulfillment_support.json](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/config/fulfillment_support.json).

Prepare directories and validate enabled integrations:

```bash
python3 scripts/run_fulfillment_support.py
```

Validate without creating directories:

```bash
python3 scripts/run_fulfillment_support.py --validate-only
```

The environment currently models:

- Slack-first intake with optional Gmail/Shopify extensions
- Labelogics-first shipment lookup with Shopify as backup when credentials exist
- escalation-owner routing plus Slack operator tagging
- a scheduled agent window of every 2 hours on weekdays from 8:00 AM through 6:00 PM America/Denver

The default config is currently set for a Slack plus Labelogics first pass:

- Slack enabled
- Labelogics enabled
- Gmail disabled
- Shopify disabled

The Slack side now assumes live Slack Web API access rather than a file export.

The Labelogics side now assumes a production app URL, sandbox URL, and API key/password, with token generation performed through the documented auth endpoint. Account selection is handled through a matching layer instead of one hard-coded global account ID.

The matching layer now persists reviewable connection data in a local SQLite catalog under `support-agent/knowledge/connections.sqlite3`.

The same SQLite catalog now also stores support cases, case events, and case assignments so the agent can:

- group work by Slack thread
- post customer-facing replies in-thread
- escalate unresolved issues into `#fulfillment-ops`
- keep cases open until a human marks them resolved

There is now also a read-only review pipeline for the future `agent.anatainc.com` page. It collects live candidate support threads and outputs stable JSON/Markdown/HTML artifacts for UI consumption.

The internal app now exposes that review surface inside the admin site at:

- `/admin/fulfillment-cs/`
- `/admin/fulfillment-cs/reports/`
- `/admin/fulfillment-cs/reports/latest`

Run the review pipeline:

```bash
python3 scripts/run_support_agent_review.py
```

Dry-run it without writing artifacts:

```bash
python3 scripts/run_support_agent_review.py --dry-run
```

If the hosted app should read support-review artifacts from persistent storage, set `SUPPORT_AGENT_REPORTS_DIR` to that live reports directory.

The Shopify side is scaffolded as an optional backup order-lookup path using an admin-created custom app, but it remains disabled until real store credentials are provided.

Run the scheduled agent logic manually:

```bash
python3 scripts/run_fulfillment_support.py --run-agent
```

Force a manual run outside the scheduled work window:

```bash
python3 scripts/run_fulfillment_support.py --run-agent --force-run
```

Setup notes live in [docs/fulfillment-support-production-setup.md](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/docs/fulfillment-support-production-setup.md).

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
