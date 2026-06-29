# HubSpot Sales System Spec

Date: 2026-06-29
Scope: HubSpot-first sales operations, deck generation, lead audit rules, and `agent.anatainc.com` structure

## Purpose

Replace the prior ClickUp-centric sales support flow with a HubSpot-first operating system that:

- keeps CRM ownership in HubSpot
- generates service-specific sales decks from a guided visual flow
- creates or updates the right commercial records for each lead
- enforces lead and deal data quality through deterministic audit rules
- exposes the full operating flow inside `agent.anatainc.com`

## Core Decision

HubSpot becomes the system of record for:

- contacts
- companies
- deals
- commercial status
- sales-stage progression
- shareable commercial outputs tied to a prospect

`agent.anatainc.com` becomes the operator surface for:

- intake review
- deck generation
- audit review
- rule execution
- task follow-through
- reporting and queue management

## Deal vs Quote

Use this working model:

- `Deal`
  - the sales opportunity record
  - owns pipeline stage, owner, expected value, close date, service line, and operational status
  - should exist for every qualified lead we are actively pursuing

- `Quote`
  - the commercial document sent to the buyer once pricing and scope are ready
  - should be created only after the deal is qualified enough for pricing
  - should stay associated to the deal, contact, and company

Working rule:

- every real opportunity gets a deal
- not every deal gets a quote
- quotes are optional until pricing is ready

## Recommended Object Model

### HubSpot native objects

- `Contact`
  - buyer or stakeholder
- `Company`
  - brand or business account
- `Deal`
  - active opportunity
- `Quote`
  - pricing/proposal document when commercial scope is ready

### Anata-managed artifacts

- `Sales deck`
  - strategy or commercial narrative artifact
  - may be marketing, fulfillment, or ads focused
- `Audit artifact`
  - structured assessment output for fulfillment, marketing, or ads

### Recommended association model

- one company can have many contacts
- one company can have many deals
- one deal can have many contacts
- one deal can have zero or many decks
- one deal can have zero or many audits
- one deal can have zero or many quotes, but only one active quote at a time

## Service Tracks

The system should support separate but linked service tracks:

- `marketing`
- `fulfillment`
- `ads`

Each deal should declare:

- primary service line
- secondary service line if needed
- current delivery interest
- deck type required
- audit type required

## Target Workflow

### 1. Lead intake

Input can come from:

- website lead form
- manual lead entry
- outbound sourced lead
- referral lead

System actions:

- upsert contact
- upsert company
- associate contact to company
- create or reuse an open deal
- stamp lead source and service interest

### 2. Qualification audit

Run a deterministic audit against the lead and deal.

Goals:

- confirm required fields exist
- normalize service type
- detect duplicates
- detect missing owner
- detect missing next step
- detect missing commercial artifact

### 3. Deck generation

An operator chooses a guided path in `agent.anatainc.com`.

The path should collect:

- service line
- lead stage
- company context
- prospect pain points
- recommended offer
- pricing posture
- deck template type

Output:

- generated deck artifact
- linked deck URL or file reference
- deck summary written back to the deal

### 4. Commercial record creation

After deck creation:

- if the lead is still exploratory, update the deal only
- if scope and pricing are ready, create or update a quote tied to the deal

### 5. Share-ready sync

Once the deck or quote is ready:

- store the share URL on the deal
- stamp the latest commercial artifact type
- stamp artifact status
- stamp sent date
- stamp follow-up due date

### 6. Ongoing automation

Run recurring checks for:

- stale deals
- missing next steps
- deals without associated contacts
- deals without associated companies
- service mismatch between deal and artifact
- deals missing required audit outputs
- decks generated but never sent
- quotes created but not followed up

## Rule Framework

Rules should be grouped by object and stage.

### Contact rules

- first name required for human contacts when available
- email or phone required
- lifecycle stage must be set
- contact must be associated to a company for B2B opportunities

### Company rules

- company name required
- website domain preferred
- service interest required once qualified
- owner required before proposal work starts

### Deal rules

- deal name required
- pipeline and stage required
- associated company required
- at least one associated contact required
- service line required
- source required
- owner required
- next step required after qualification
- estimated value or pricing posture required before quote creation

### Deck rules

- deck type required
- deck URL required once generated
- deck status required
- one current primary deck per deal and service line

### Quote rules

- quote allowed only for deals in pricing-ready stages
- quote must inherit the correct company and primary contact association
- quote amount must align with the current deal amount or intentional override
- only one active share-ready quote per deal unless explicitly versioned

### Audit rules

- fulfillment leads require fulfillment readiness fields
- marketing leads require channel goals and current growth constraints
- ads leads require spend context, channel scope, and conversion target

## Proposed Required Fields

### Deal-level required fields

- `anata_service_line`
- `anata_solution_type`
- `anata_lead_source_detail`
- `anata_pipeline_owner`
- `anata_next_step`
- `anata_next_step_due_at`
- `anata_primary_deck_type`
- `anata_primary_deck_url`
- `anata_primary_audit_type`
- `anata_primary_audit_url`
- `anata_commercial_status`
- `anata_handoff_status`

### Useful computed fields

- `anata_record_health`
- `anata_missing_fields`
- `anata_last_audit_at`
- `anata_last_artifact_sent_at`
- `anata_days_since_last_touch`

## Automated Sales Tools

The automation layer should do four jobs:

### 1. Hygiene

- fix or flag incomplete records
- normalize field values
- enforce association rules

### 2. Momentum

- find stale deals
- prompt follow-up
- flag missing next steps
- escalate overdue opportunities

### 3. Commercial readiness

- detect whether a lead is ready for a deck
- detect whether a deal is ready for a quote
- detect whether the wrong artifact was created for the stage

### 4. Handoff readiness

- confirm the fulfillment or marketing handoff fields are complete
- confirm the shareable artifact exists
- confirm scope is visible to the team

## `agent.anatainc.com` Structure

The site should be organized around operator work, not generic reporting.

### Primary sections

- `/admin/sales/`
  - what is happening now
  - what is blocked
  - what should happen next

- `/admin/sales/leads/`
  - lead queue
  - lead audit failures
  - duplicate and incomplete records

- `/admin/sales/deals/`
  - open pipeline
  - stale deal queue
  - deal health status

- `/admin/sales/decks/`
  - deck generation flow
  - deck library
  - deck status by deal

- `/admin/sales/quotes/`
  - quote-ready deals
  - sent quotes
  - follow-up queue

- `/admin/sales/audits/`
  - fulfillment audits
  - marketing audits
  - ads audits

- `/admin/sales/rules/`
  - object definitions
  - rule catalog
  - audit failures
  - rule history

- `/admin/sales/handoffs/`
  - fulfillment handoff queue
  - marketing handoff queue
  - blocked handoffs

## Execution Order

Build in this order:

1. define HubSpot object and field rules
2. build lead and deal audit engine
3. build deck-generation workflow
4. add quote creation rules
5. add audit artifact support for fulfillment and marketing
6. add ads audit support
7. expose all of it in `agent.anatainc.com`

## Current Recommendation

Do not start with quotes first.

Start with:

1. HubSpot deal model
2. required associations
3. rule engine
4. deck generation and deck-to-deal sync

Then layer quotes on top once the deal model is clean.
