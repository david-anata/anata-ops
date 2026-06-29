# Fulfillment Support Environment Setup

## Objective

Prepare a Codex-cloud-friendly environment for a fulfillment customer service agent that:

- reviews new customer service requests from Slack through the live Slack API
- looks up shipment and warehouse details in Labelogics
- drafts customer-ready replies
- escalates unresolved or risky issues to a human owner

This document now covers both the environment build and the scheduled support-agent runtime.

The current default build in this repo is intentionally narrowed to:

- Slack intake enabled
- Labelogics enabled
- Gmail disabled
- Shopify disabled
- `#fulfillment-ops` as the internal escalation queue
- weekday-only runs at `08:00, 10:00, 12:00, 14:00, 16:00, 18:00` America/Denver

## Environment Contract

The environment runner is:

```bash
python3 scripts/run_fulfillment_support.py
```

It can do three things:

1. creates the support-agent working directories
2. validates that the enabled integrations have the required environment variables
3. runs the live support-agent workflow when you pass `--run-agent`

Run validation only:

```bash
python3 scripts/run_fulfillment_support.py --validate-only
```

Run the live agent:

```bash
python3 scripts/run_fulfillment_support.py --run-agent
```

Force a manual run outside the scheduled window:

```bash
python3 scripts/run_fulfillment_support.py --run-agent --force-run
```

Run the read-only review pipeline that prepares candidate threads for the future dashboard:

```bash
python3 scripts/run_support_agent_review.py
```

## Required Files

- config at `config/fulfillment_support.json`
- env template at `.env.example`

Default working directories:

- `support-agent/intake`
- `support-agent/runs`
- `support-agent/knowledge`
- `support-agent/escalations`

Review database:

- `support-agent/knowledge/connections.sqlite3`

The SQLite catalog now includes:

- connection review tables
- `support_cases`
- `support_case_events`
- `support_case_assignments`

The read-only review pipeline also writes UI-friendly artifacts under the support-agent reports directory:

- JSON
- Markdown
- HTML

## Required Environment Variables

Slack intake:

- `SLACK_API_BASE_URL`
- `SLACK_BOT_TOKEN`
- `SUPPORT_SLACK_CHANNELS`

Gmail intake if you enable it later:

- `SUPPORT_GMAIL_EXPORT_PATH`

Shopify lookup if you enable it later:

- `SHOPIFY_STORE_DOMAIN`
- `SHOPIFY_ADMIN_API_ACCESS_TOKEN`
- `SHOPIFY_API_VERSION`

Labelogics lookup:

- `LABELOGICS_APP_URL`
- `LABELOGICS_SANDBOX_URL`
- `LABELOGICS_KEY`
- `LABELOGICS_PASSWORD`

Optional but recommended:

- `LABELOGICS_API_DOCS_URL`
- `SUPPORT_ESCALATION_DEFAULT_OWNER`
- `SUPPORT_ESCALATION_SLACK_USER_IDS`
- `SUPPORT_TIMEZONE`
- `SUPPORT_WORKSPACE_ROOT`
- `SUPPORT_AGENT_ROOT`

## Codex Cloud Direction

For Codex automations, point the automation at this workspace and run the environment validator before enabling the recurring customer service job. That keeps schedule failures obvious when a connector path or token is missing.

Recommended order:

1. populate the required environment variables
2. run `python3 scripts/run_fulfillment_support.py --validate-only`
3. confirm the output status is `ready`
4. create the recurring Codex automation on top of this workspace

## Labelogics References

- app: `https://app.labelogics.com`
- sandbox: `https://sandbox.labelogics.com`
- docs: `https://app.labelogics.com/apidocs`
- intro: `https://app.labelogics.com/apidocs/get-started/introduction`
- token flow docs: `https://app.labelogics.com/apidocs/get-started/get-tokens`
- shipment add docs: `https://app.labelogics.com/apidocs/shipment/add`
- label purchase docs: `https://app.labelogics.com/apidocs/label/purchase`
- label track docs: `https://app.labelogics.com/apidocs/label/track`
- label void docs: `https://app.labelogics.com/apidocs/label/void`
- order add docs: `https://app.labelogics.com/apidocs/order/add`
- order tracking docs: `https://app.labelogics.com/apidocs/order/tracking`

These links tell us the relevant operational surface, but the docs app is JavaScript-rendered. From the page structure, I can confirm the documented areas include token generation plus shipment, label, and order workflows. The exact request paths and auth-header shape still need to be validated from the rendered docs UI or a working sample request.

Confirmed from the docs bundle:

- token route: `POST /api/auth/tokens/generate`
- token auth: `Authorization: Basic base64(API_KEY:API_PASSWORD)`
- standard account headers for operational calls:
  - `Authorization: Bearer {token}`
  - `AccountID: {Account ID}`

The support environment no longer assumes one global `LABELOGICS_ACCOUNT_ID`. Instead, it expects account matching logic to reconcile:

- Slack channel brand identity
- Shopify store identity
- Labelogics account identity

using normalized names plus alias overrides.

Default knowledge paths:

- `support-agent/knowledge/connections.sqlite3`
- `support-agent/knowledge/shopify_accounts.json`
- `support-agent/knowledge/labelogics_accounts.json`

Each file should contain a JSON array of records with stable IDs, display names, and optional aliases.

Example Shopify record:

```json
{
  "store_domain": "brand-store.myshopify.com",
  "shop_name": "Brand Store",
  "aliases": ["brand", "brand-store"]
}
```

Example Labelogics record:

```json
{
  "account_id": "uuid-here",
  "account_name": "Brand Fulfillment",
  "aliases": ["brand", "brand-fulfillment"]
}
```

## Shopify Backup Lookup

Shopify is currently disabled in the default config, but the environment is ready to support it as a backup order-verification source once you provide custom-app credentials.

Recommended Shopify env values:

```bash
SHOPIFY_STORE_DOMAIN=your-store.myshopify.com
SHOPIFY_ADMIN_API_ACCESS_TOKEN=your-admin-token
SHOPIFY_API_VERSION=2026-01
```

Recommended scopes from Shopify's official docs:

- `read_orders`
- `read_all_orders` if you need orders older than 60 days

Shopify’s official docs also note:

- Admin GraphQL requests are versioned at `/admin/api/{api_version}/graphql.json`
- access scopes can be checked at `/admin/oauth/access_scopes.json`

## Minimal First Pass

For the current Slack plus Labelogics setup, start with:

```bash
SUPPORT_TIMEZONE=America/Denver
SLACK_API_BASE_URL=https://slack.com/api
SLACK_BOT_TOKEN=xoxb-your-token
SUPPORT_SLACK_CHANNELS=customer-support,fulfillment,ops
LABELOGICS_APP_URL=https://app.labelogics.com
LABELOGICS_SANDBOX_URL=https://sandbox.labelogics.com
LABELOGICS_API_DOCS_URL=https://app.labelogics.com/apidocs
LABELOGICS_KEY=your-key
LABELOGICS_PASSWORD=your-password
SUPPORT_ESCALATION_DEFAULT_OWNER=yourname@company.com
SUPPORT_ESCALATION_SLACK_USER_IDS=U_VON,U_ASHLEY
```

Then run:

```bash
python3 scripts/run_fulfillment_support.py --validate-only
python3 scripts/run_fulfillment_support.py
python3 scripts/run_fulfillment_support.py --run-agent
```

## Agent Behavior

The live agent uses one Slack thread as one case.

- A new thread creates a new support case.
- New human replies on the same thread update the existing case.
- The bot ignores its own prior messages and duplicate Slack events.
- If it can resolve the case confidently from Labelogics, it replies in the same thread and logs the evidence.
- If Shopify credentials are later added and a store match exists, Shopify becomes a secondary verification source.
- If evidence is missing, conflicting, or the brand/account match is missing, the bot replies in-thread that it is looking into it, posts a structured escalation into `#fulfillment-ops`, tags the configured Slack users, and keeps the case open.
- A human can mark the case resolved by replying with a resolution marker such as `resolved`.

## Future Dashboard Shape

The new review pipeline is intended to back another page on `agent.anatainc.com`.

The internal app can now render those artifacts directly in the admin site at:

- `/admin/fulfillment-cs/`
- `/admin/fulfillment-cs/reports/`
- `/admin/fulfillment-cs/reports/latest`

Its output includes, for each candidate thread:

- Slack channel
- permalink
- question summary
- extracted identifiers
- matched brand / connection status
- current evidence from Labelogics and Shopify
- recommended action
- draft customer reply

If the hosted app should read support-review artifacts from a persistent disk, set:

```bash
SUPPORT_AGENT_REPORTS_DIR=/var/data/support_agent/runs
```

Recommended Slack connectivity checks:

```bash
curl -sS -X POST https://slack.com/api/auth.test \
  -H "Authorization: Bearer $SLACK_BOT_TOKEN"
```

```bash
curl -sS -X POST https://slack.com/api/conversations.list \
  -H "Authorization: Bearer $SLACK_BOT_TOKEN" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data "types=public_channel,private_channel"
```

Recommended Labelogics token check:

```bash
curl -sS -X POST https://app.labelogics.com/api/auth/tokens/generate \
  -H "Authorization: Basic $(printf '%s' "$LABELOGICS_KEY:$LABELOGICS_PASSWORD" | base64)"
```
