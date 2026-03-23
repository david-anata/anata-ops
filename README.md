# AP Audit Agent

Controller-grade AP audit CLI for comparing transaction exports against ClickUp AP tasks, producing daily Slack warnings, weekly review outputs, schema-gap reporting, and optional low-risk ClickUp updates.

## Easiest Weekly Workflow

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

If a token has been pasted into chat or terminal history, rotate it immediately and replace it with a fresh secret in your secret manager.
