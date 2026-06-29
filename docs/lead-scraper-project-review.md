# Lead Scraper / Sales Support Agent / Website Ops Review

Date: 2026-03-28  
Scope: current repo and live operating model for `Lead-scraper`, `sales_support_agent`, and embedded `Website Ops`

## Executive Summary

This project is no longer a single-purpose lead scraper. It is now a shared operational platform with three distinct but related systems:

1. `Lead Scraper`
   - outbound lead sourcing for ecommerce brands
2. `Sales Support Agent`
   - post-lead ClickUp workflow support, communication tracking, stale lead prevention, and deck generation
3. `Website Ops`
   - SEO and AI-search optimization control surface embedded into the agent admin on `agent.anatainc.com`

The repo is operationally powerful, but the context window is crowded because these three systems live together. The strongest recent evolution is `Website Ops`, which has shifted from a structural page checker into a data-driven optimization engine that uses:

- Google Search Console for demand, CTR, and query language
- GA4 for landing-page sessions and lead-event trust
- WordPress REST + Elementor data for deterministic execution

The platform is now capable of:

- generating daily website recommendations
- auto-executing high-confidence deterministic changes
- preserving approval state and execution history
- surfacing queue, reports, and system status in the internal dashboard

The biggest structural reality is this: the codebase is a multi-product operations repo, not a narrowly bounded app. That means it needs stronger documentation and operational discipline than a normal single-use service.

## What This Repo Actually Is

### Product boundary

This repo currently contains:

- a legacy lead-generation API in [main.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/main.py)
- a modular FastAPI application in [sales_support_agent/main.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/main.py)
- a Website Ops subsystem inside:
  - [sales_support_agent/services/website_ops.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/services/website_ops.py)
  - [sales_support_agent/services/website_ops_autonomy.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/services/website_ops_autonomy.py)
  - [sales_support_agent/services/website_ops_vendor/core.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/services/website_ops_vendor/core.py)
  - [sales_support_agent/services/website_ops_vendor/executor.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/services/website_ops_vendor/executor.py)

### Operational meaning

This is an internal operating system for revenue and growth work, not just an application server.

It spans:

- lead generation
- post-lead follow-up operations
- internal admin dashboards
- Google/WordPress-connected SEO operations
- reporting and execution history

## Business Use Case

### Lead Scraper

Purpose:

- source new ecommerce prospects
- enrich them with Apollo
- push usable outreach lists into downstream workflows

Main outcomes:

- CSV exports
- Slack delivery
- optional Instantly / HeyReach handoff

### Sales Support Agent

Purpose:

- support the period after a lead already exists in ClickUp
- keep follow-up from stalling
- centralize activity logging and reminders

Main outcomes:

- stale lead scans
- mailbox intake
- daily digests
- Slack alerts
- operational audit trail

### Website Ops

Purpose:

- increase qualified organic leads from `anatainc.com`
- improve Google and AI-search visibility for service pages
- automate safe, data-backed page improvements

Main outcomes:

- daily SEO/AI-search recommendations
- approval queue
- auto-executed high-confidence changes
- daily/weekly/monthly reports

## Current Infrastructure

### Deployment model

Render blueprint in [render.yaml](/Users/davidnarayan/Documents/Playground/Lead-scraper/render.yaml):

- web service: `sales-support-agent`
- cron: `sales-support-stale-scan`

The Website Ops UI is mounted inside the same agent surface, not deployed as a separate app.

### Public/internal surfaces

Public/internal host references in the codebase:

- `agent.anatainc.com`
- `sales-support-agent.onrender.com`
- `anatainc.com`

Website Ops routes in [main.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/main.py):

- `/admin/website-ops`
- `/admin/website-ops/queue`
- `/admin/website-ops/reports`
- `/admin/website-ops/reports/latest`
- `/admin/website-ops/feedback/{feedback_id}`
- `/admin/api/website-ops/run`
- `/admin/api/website-ops/status`
- `/admin/api/website-ops/feedback`
- `/admin/api/website-ops/feedback/{feedback_id}/review`

### Storage model

Website Ops uses filesystem-backed runtime storage under `WEBSITE_OPS_ROOT`.

It stores:

- reports
- feedback items
- backups
- run state

Important implication:

- if Render does not use persistent disk for `WEBSITE_OPS_ROOT`, reports, queue state, and backups are vulnerable to restart loss

### Current Website Ops runtime assumptions

Expected env/config:

- `WEBSITE_OPS_ROOT`
- `WEBSITE_OPS_URLS`
- `WEBSITE_OPS_EXECUTE_APPROVED`
- `WEBSITE_OPS_GSC_PROPERTY`
- `WEBSITE_OPS_GA4_PROPERTY_ID`
- `WEBSITE_OPS_GA4_PRIMARY_LEAD_EVENT`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `WP_SITE_URL`
- `WP_USERNAME`
- `WP_APPLICATION_PASSWORD`

## Integrations and Dependencies

### Lead and sales-side integrations

- ClickUp
- Slack
- Apollo
- StoreLeads
- Instantly
- HeyReach
- Gmail
- Canva
- Google Sheets

### Website Ops integrations

- Google Search Console API
- Google Analytics Data API
- Google Analytics Admin API
- WordPress REST API
- Elementor page data via WordPress REST meta
- GoDaddy and DNS, operationally outside this repo

## Website Ops Architecture

### Functional layers

1. `crawl / inspect`
   - [core.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/services/website_ops_vendor/core.py)
   - fetches pages
   - extracts title, H1, canonical, status, indexability signals
   - generates page observations and structural issues

2. `decision engine`
   - [website_ops_autonomy.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/services/website_ops_autonomy.py)
   - pulls Search Console and GA4
   - scores pages
   - generates actions with evidence, confidence, trust state, and execution eligibility

3. `workflow and reporting`
   - [website_ops.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/services/website_ops.py)
   - syncs queue items
   - preserves approval state
   - renders dashboard, queue, detail pages, and report views

4. `execution`
   - [executor.py](/Users/davidnarayan/Documents/Playground/Lead-scraper/sales_support_agent/services/website_ops_vendor/executor.py)
   - executes deterministic changes against WordPress / Elementor
   - creates backups
   - verifies live state after mutation

### Supported Website Ops actions

Current executable action types:

- `replace_primary_heading`
- `rewrite_title_and_intro`
- `strengthen_primary_cta`
- `add_internal_links`
- `update_faq_ai_extraction`

### Auto-execution policy

Current policy:

- high-confidence deterministic actions may auto-execute
- lower-confidence actions remain approval-first
- execution requires resolvable target regions and verification rules

### Analytics trust model

GA4 now operates on a primary lead-event contract:

- expected default: `generate_lead`

Trust states:

- `trusted`
- `partial`
- `missing`

This matters because CTA/conversion recommendations should not auto-execute from weak or ambiguous conversion data.

## Current Website Ops Goal

The working goal in code is:

- improve qualified organic leads by prioritizing service pages with the strongest search opportunity, weakest conversion efficiency, and clearest safe mutation path

Operationally, that translates to:

- more impressions turning into clicks
- more landing sessions turning into lead events
- stronger page clarity for both Google and AI extraction

## How Daily Operations Are Supposed To Work

### Daily flow

1. daily Website Ops run is triggered
2. monitored service pages are fetched and inspected
3. Search Console and GA4 data are pulled
4. action queue is generated from evidence
5. safe high-confidence items may auto-execute
6. all report artifacts are written
7. team reviews anything still requiring approval

### Team-facing surfaces

Primary surfaces:

- [agent.anatainc.com/admin/website-ops](https://agent.anatainc.com/admin/website-ops)
- [agent.anatainc.com/admin/website-ops/queue](https://agent.anatainc.com/admin/website-ops/queue)
- [agent.anatainc.com/admin/website-ops/reports/latest](https://agent.anatainc.com/admin/website-ops/reports/latest)

### Decision logic

Pages are prioritized from combined evidence:

- search demand
- weak CTR
- weak lead conversion
- structural SEO risk
- thin topical or cluster support

The system is intentionally no longer “checklist SEO.” It is an optimization engine with execution attached.

## Context Window Review

### What makes the context window heavy

This repo asks one codebase to hold:

- outbound lead sourcing
- sales support operations
- dashboard UI
- report generation
- WordPress execution
- Google analytics integration
- SEO automation

That creates context dilution.

The main risk is not technical impossibility. It is operator confusion.

### Current context strengths

- one repo controls a large amount of operational leverage
- the Website Ops work is now substantially more structured than before
- most important runtime pieces are explicit in code

### Current context weaknesses

- repo purpose is broader than its name
- multiple systems live under one deployment story
- some behavior is documented in several places instead of one canonical system brief
- “lead scraper” naming no longer matches actual platform scope

### Best interpretation

Treat this repo as:

- `Anata Revenue Ops Platform`

and treat `Lead Scraper` as only one subsystem inside it.

## Current Operational Risks

### 1. Repo identity mismatch

The repo name and historical framing understate what it now contains.

Risk:

- onboarding confusion
- wrong assumptions about ownership
- fragile changes caused by partial understanding

### 2. Shared deployment surface

Website Ops is embedded into the same agent app used for other admin operations.

Risk:

- broader blast radius for deploy mistakes
- config coupling between unrelated features

### 3. Filesystem-backed state

Website Ops stores queue/report/backups on disk.

Risk:

- restart loss without persistent storage

### 4. WordPress/Elementor determinism

Execution depends on stable widget discovery.

Risk:

- template/layout drift can reduce safe mutation coverage

### 5. GA4 trust dependency

Conversion prioritization is only as good as the primary lead event implementation.

Risk:

- false confidence in CTA decisions if `generate_lead` is not mapped cleanly

### 6. Dual app entrypoints

There is:

- top-level `main.py`
- modular `sales_support_agent/main.py`

Risk:

- confusion over which app is authoritative for which route

## Current Strengths

### 1. The Website Ops workflow is materially real now

It is not just a report generator anymore.

It now has:

- a queue
- approval state persistence
- run state persistence
- deterministic execution
- analytics-backed recommendations

### 2. The admin surface is unified

Website Ops now lives under the same internal surface the team already uses.

### 3. The execution model is defensible

It uses:

- backups
- verification
- approval boundaries
- trust states

That is the correct shape for autonomous content/SEO operations.

## Team Operating Model

### Leadership

Should care about:

- qualified organic leads
- page-level improvement velocity
- how much of the queue is auto-executing vs waiting on manual approval

### Operators

Should use:

- dashboard
- queue
- latest report

to review:

- what changed
- why it changed
- what still needs approval

### Technical owner

Should monitor:

- env correctness
- persistent storage
- Google API access
- WordPress application password validity
- executor coverage drift

## What The Project Should Keep Doing

- keep using Search Console as the source of query language and demand
- keep using one primary GA4 lead event for conversion prioritization
- keep auto-executing only deterministic high-confidence actions
- keep persisting queue state and execution history
- keep the dashboard as the operating surface, not a separate spreadsheet or ad hoc thread

## What The Project Should Stop Doing

- stop behaving like Website Ops is a sidecar utility
- stop relying on “visit the page to remember to run it” as the only scheduler model
- stop treating all conversion data as equally trustworthy
- stop using repo naming/documentation that implies this is only a scraper

## What Needs To Happen Next

### Immediate

- make sure the daily sweep is truly scheduled, not just visit-triggered
- ensure `WEBSITE_OPS_ROOT` is on persistent disk
- confirm the live service is actually running with auto-execution enabled
- validate the GA4 primary lead event on a real production submit

### Next growth layer

- widen deterministic execution coverage only where widget targeting stays safe
- add stronger weekly executive rollups
- add clearer “why this page now / why this action / why safe” narrative in reports if leadership needs more visibility

### Longer-term

- decide whether this repo should be renamed or documented as a revenue/growth operations platform
- decide whether Website Ops eventually deserves its own service boundary instead of remaining embedded

## Bottom Line

This project is no longer best understood as `Lead Scraper`.

It is an internal growth operations platform that currently combines:

- outbound lead generation
- sales support operations
- SEO / AI-search optimization
- admin workflow tooling

That breadth is both its advantage and its main complexity cost.

The most important current subsystem, strategically, is `Website Ops`. It is now close to being a real autonomous website operator, but its effectiveness still depends on:

- trustworthy GA4 lead measurement
- stable WordPress execution targets
- persistent runtime storage
- consistent daily scheduling

If those four stay healthy, the system can compound improvements instead of just reporting issues.
