# Render Setup: Website Ops as Source of Truth

## Purpose

This setup makes the live Website Ops dashboard inside `agent.anatainc.com` the single source of truth for:

- reports
- approvals
- feedback records
- execution history
- backups of approved changes

It also explains where auto-optimization starts today and how GA4 and Google Search Console fit into the next phase.

## 1. What Auto-Optimization Means Right Now

The current live system is not fully autonomous content optimization yet.

What it does today:

- audits monitored URLs
- detects structural SEO issues
- stores feedback records
- lets the team approve or reject suggested actions
- executes approved safe actions
- verifies the live result
- stores the result in the dashboard record

### Current auto-execution boundary

Current automation is:

- approval-based
- deterministic
- limited to safe actions

Current supported live action:

- `replace_primary_heading`

That means the workflow today is:

1. system identifies issue
2. team reviews in dashboard
3. team approves action
4. system executes action
5. system verifies result
6. system records outcome

This is controlled automation, not blind automation.

## 2. Where GA4 and Google Search Console Fit

GA4 and GSC are part of the intended decision layer, not the current execution layer.

### Planned role of Google Search Console

Search Console should drive:

- impressions by page
- clicks by page
- CTR by page
- query/page opportunities
- pages losing visibility
- pages gaining impressions but underperforming

Website Ops should use that data to generate:

- refresh recommendations
- title and heading suggestions
- internal-link opportunities
- cluster expansion ideas

### Planned role of GA4

GA4 should drive:

- landing page engagement
- lead-form conversion signals
- page-level conversion patterns
- high-traffic / low-conversion page detection
- CTA weakness detection

Website Ops should use that data to generate:

- conversion-focused page revisions
- CTA improvement recommendations
- layout priority suggestions
- service-page opportunity scoring

### Important current-state note

At this moment:

- WordPress execution is live
- Website Ops dashboard is live
- feedback / approval / reporting is live
- GA4 and GSC are **not yet wired into the live Website Ops runner**

So in the current system:

- SEO auditing is live
- approval-based execution is live
- analytics-informed optimization is planned, but not yet implemented in code

## 3. What Must Be True For The Dashboard To Be The Source of Truth

If the dashboard is the source of truth, the live service must persist:

- reports
- feedback records
- backups
- execution history

That means the Website Ops storage path cannot stay on ephemeral Render disk.

It should use a Render persistent disk.

## 4. Required Render Service

Use the existing live service that serves:

- `https://agent.anatainc.com`

Do **not** create a second website-ops service for the same domain.

Website Ops lives inside the existing agent/admin app.

## 5. Required Persistent Disk

Attach a persistent disk to the live `agent.anatainc.com` service.

Recommended settings:

- Disk name: `website-ops-data`
- Mount path: `/var/data`

Then Website Ops should write to:

- `/var/data/website_ops/reports`
- `/var/data/website_ops/feedback`
- `/var/data/website_ops/backups`

## 6. Exact Environment Variables

Set these on the live Render service behind `agent.anatainc.com`.

### Core Website Ops

```env
WEBSITE_OPS_ROOT=/var/data/website_ops
WEBSITE_OPS_DAILY_REPORTS_DIR=/var/data/website_ops/reports/daily
WEBSITE_OPS_FEEDBACK_DIR=/var/data/website_ops/feedback
WEBSITE_OPS_BACKUPS_DIR=/var/data/website_ops/backups
WEBSITE_OPS_EXECUTE_APPROVED=true
WEBSITE_OPS_URLS=https://anatainc.com/,https://anatainc.com/services/,https://anatainc.com/services/fulfillment/,https://anatainc.com/services/shipping/,https://anatainc.com/services/ai/,https://anatainc.com/services/advertising/,https://anatainc.com/contact/
WEBSITE_OPS_REPORT_TITLE=Anata Website Ops Daily Report
WEBSITE_OPS_TIMEOUT_SECONDS=20
WEBSITE_OPS_USER_AGENT=anata-website-ops/1.0
```

### WordPress Execution

```env
WP_SITE_URL=https://anatainc.com
WP_USERNAME=anatainc
WP_APPLICATION_PASSWORD=<current-wordpress-application-password>
```

### Recommended Future Analytics Variables

These are for the next phase when GA4 and GSC ingestion is wired into code.

```env
GOOGLE_SERVICE_ACCOUNT_JSON=<full-json-service-account-key>
WEBSITE_OPS_GSC_PROPERTY=sc-domain:anatainc.com
WEBSITE_OPS_GA4_PROPERTY_ID=372887830
```

## 7. Why This Storage Layout Matters

Without persistent storage:

- reports disappear on redeploy
- approvals become unreliable as history
- backups are not durable
- the dashboard stops being a trusted operational system

With persistent storage:

- daily reports remain visible
- approved actions remain traceable
- rollback evidence remains available
- the team can use the dashboard as the system of record

## 8. Exact Render Click Path

### Add the persistent disk

1. Open the Render service behind `agent.anatainc.com`
2. Open `Disks`
3. Add a persistent disk
4. Name it `website-ops-data`
5. Set mount path to `/var/data`
6. Save

### Add environment variables

1. Open `Environment`
2. Add the Website Ops env vars
3. Add the WordPress execution env vars
4. Save changes
5. Trigger redeploy

## 9. What The Team Gets After This Setup

Once this is configured, the live dashboard becomes the source of truth for:

- latest reports
- historical reports
- feedback queue
- approved actions
- execution results
- backups

Team URLs:

- `https://agent.anatainc.com/admin/website-ops`
- `https://agent.anatainc.com/admin/website-ops/queue`
- `https://agent.anatainc.com/admin/website-ops/reports/latest`

## 10. What Still Needs To Be Built After This

To make Website Ops analytics-driven instead of only structure-driven, the next implementation layer should:

1. read GSC page/query data daily
2. read GA4 landing-page and lead-conversion data daily
3. store analytics snapshots in persistent Website Ops storage
4. enrich reports with:
   - CTR opportunities
   - conversion weak spots
   - rising pages
   - declining pages
5. generate approval-ready recommendations from that data

That is the point where Website Ops becomes:

- structural SEO system
- conversion optimization system
- analytics-informed page improvement system

instead of only a page-quality checker plus execution queue.

## 11. Recommended Operating Model

### Daily

- run Website Ops
- review latest report
- approve safe actions
- verify results

### Weekly

- review unresolved issues
- review cluster gaps
- review candidate pages from GSC and GA4
- set next content and optimization priorities

### Monthly

- route architecture review
- internal-link review
- cluster expansion review
- stale page refresh planning

## 12. Executive Summary

The current live Website Ops system already supports:

- page auditing
- dashboard review
- feedback intake
- approval workflow
- safe auto-execution

To make it the permanent operational source of truth, the only required production step is:

- persistent disk + live environment configuration

To make it analytics-driven, the next build step is:

- GA4 + GSC ingestion into the daily and weekly reporting layer
