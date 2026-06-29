# Website Ops Autonomy Spec

## Core Goal

Website Ops exists to increase qualified inbound leads from organic search and AI-driven discovery while reducing manual website operations.

That means the system should optimize for:

- more qualified organic visibility
- more service-page traffic with buyer intent
- more lead conversions from those pages
- faster improvement cycles with less manual intervention

It should not optimize for vanity activity.

It should optimize for measurable business outcomes.

## Primary Operating Objective

The system should continuously answer five questions:

1. what pages are helping growth
2. what pages are underperforming
3. what changes should be made next
4. what should stop being changed
5. what the team should do more of to help the system win

## Required System Behavior

Website Ops should become an automated decision-and-execution loop with human approval only where needed.

The loop should be:

1. ingest data
2. detect opportunities and failures
3. score pages
4. recommend actions
5. auto-execute safe actions
6. request approval for higher-impact actions
7. verify results
8. report clearly
9. repeat

## Data Inputs It Must Use

### Google Search Console

Website Ops should pull, store, and use:

- page impressions
- page clicks
- page CTR
- average position
- top queries by page
- pages gaining impressions with weak CTR
- pages losing clicks or position

### Google Analytics 4

Website Ops should pull, store, and use:

- landing page sessions
- engaged sessions
- conversions by landing page
- conversion rate by landing page
- traffic trend by page
- low-conversion pages with good traffic
- pages with traffic but weak CTA performance

### Website Structure Signals

Website Ops should continuously inspect:

- H1 structure
- title tag quality
- canonical tags
- internal linking
- FAQ presence
- CTA presence
- route consistency
- AI extraction readiness

### Feedback and Approval Signals

Website Ops should use:

- team-submitted feedback
- approved actions
- rejected actions
- recurring objections
- requested page priorities

## What It Should Start Doing

Website Ops should start doing more of these automatically:

- flagging pages with rising impressions but weak CTR
- flagging pages with traffic but poor conversion
- identifying pages with structural SEO problems
- surfacing route and canonical inconsistencies
- proposing specific section-level changes
- applying safe approved changes automatically
- verifying whether a live change actually rendered
- showing expected impact of each action
- producing daily and weekly next-step queues

## What It Should Stop Doing

Website Ops should explicitly stop:

- making cosmetic edits with no clear business value
- changing pages without explaining why
- making blind edits that are not tied to search, conversion, or structural evidence
- treating all pages equally
- editing low-value pages while service pages need work
- applying high-risk copy changes without approval
- creating reports that do not answer what changed and why

## What It Should Ask The Team To Do More Of

The team should be prompted to do more of the actions that increase system effectiveness:

- approve high-confidence actions quickly
- provide proof assets like case studies, metrics, testimonials, and process detail
- resolve route/canonical decisions when multiple page families exist
- define true conversion events in GA4
- identify strategic service priorities
- review pages where the system has low confidence

The team should not need to manually inspect every page.

The team should only need to:

- approve
- reject
- clarify
- supply missing assets
- set strategic direction

## Decision Model

Each monitored page should receive a score based on:

- structural health
- search visibility
- CTR efficiency
- conversion efficiency
- service priority
- freshness / staleness

Each page should be classified into one of these buckets:

- `scale`
  - performing well, needs amplification
- `repair`
  - traffic opportunity exists but structure or messaging is weak
- `convert`
  - gets traffic but under-converts
- `expand`
  - topic deserves supporting pages
- `hold`
  - no action currently justified
- `retire-or-consolidate`
  - legacy or conflicting page architecture

## Action Categories

Every suggested or executed action should be classified as one of these:

- `structural SEO`
- `conversion improvement`
- `content clarity`
- `internal linking`
- `route / canonical`
- `cluster expansion`
- `measurement / analytics`

## Action Confidence

Every action should have a confidence level:

- `high`
  - can auto-execute if within approved safe list
- `medium`
  - should be approved in dashboard
- `low`
  - should be surfaced as a recommendation only

## Dashboard Requirements

The dashboard should not be a vague status board.

It should be an operational control system.

For each action, the dashboard should show:

- page URL
- page type
- action category
- exact section or portion of page affected
- what was there before
- what is being changed to
- why the change is recommended
- which insight triggered it
- expected business effect
- confidence level
- approval status
- execution status
- verification status
- timestamp

## Exact Dashboard Views Required

### 1. Goal View

This should answer:

- what the system is optimizing for this week
- what pages matter most right now
- what KPI movement defines success

### 2. Action Queue

This should show a concise list of actions:

- page
- section
- issue
- previous state
- new state
- why it matters
- status

### 3. Insights View

This should show:

- GSC insight
- GA4 insight
- structural website insight
- decision generated from those inputs

Examples:

- `Page has rising impressions but low CTR`
- `Page gets traffic but weak lead conversion`
- `Page has multiple H1s and weak service clarity`

### 4. Implementation Log

This should show:

- what changed
- when it changed
- whether it was approved
- whether it was verified
- which page and section changed

### 5. Team Support View

This should tell the team exactly how to help:

- approve this action
- provide proof for this page
- decide canonical route
- define conversion metric
- prioritize this cluster

## Required Action Record Format

Every action record should follow this exact structure:

- `page_url`
- `page_title`
- `action_type`
- `section_name`
- `before_state`
- `after_state`
- `reason`
- `insight_source`
- `expected_impact`
- `confidence`
- `requires_approval`
- `status`
- `approved_by`
- `executed_at`
- `verified_at`
- `verification_result`

## Automation Layers

### Layer 1: Daily Ingestion

Runs daily and pulls:

- Search Console page/query performance
- GA4 landing-page performance
- page structure checks
- existing feedback queue

### Layer 2: Daily Triage

Runs daily and classifies:

- urgent fixes
- safe auto-fixes
- approval-required changes
- watch-only pages

### Layer 3: Execution

Runs daily after triage and:

- executes approved safe actions
- stores backups
- verifies live output
- records outcome

### Layer 4: Weekly Planning

Runs weekly and:

- reviews trend movement
- identifies refresh candidates
- identifies cluster gaps
- recommends pages to expand
- recommends pages to consolidate

## Daily Output It Must Produce

Every daily report should include:

- what changed today
- what is queued next
- what improved
- what is blocked
- what the team should review

## Weekly Output It Must Produce

Every weekly report should include:

- pages gaining visibility
- pages losing visibility
- pages with CTR opportunity
- pages with conversion opportunity
- cluster expansion recommendations
- pages to stop spending time on

## What “Single Source of Truth” Means

The live dashboard should become the place where the team sees:

- all reports
- all approvals
- all pending actions
- all executed actions
- all verification results
- all support requests from the system

That means the dashboard should not just show page reports.

It should show:

- the goal
- the evidence
- the action
- the result

## Clear System Prompts For The Team

The dashboard should actively tell the team:

- `Approve this change`
- `Reject this change`
- `Provide proof for this page`
- `Resolve route conflict`
- `Prioritize this service cluster`
- `Define or confirm the conversion event`

## Final Standard

Website Ops should function like an internal growth operator.

It should know:

- what to start doing
- what to stop doing
- what to do more of
- what it needs from the team

And it should make those outputs visible in one concise dashboard with clear actions, clear reasoning, and clear results.
