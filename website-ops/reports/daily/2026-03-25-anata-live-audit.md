# Anata Live Audit

Date: 2026-03-25  
Scope: production, read-only first pass  
Method: WordPress REST API inventory + direct HTML inspection of core commercial pages

## Executive Summary

Anata does not have a content-volume problem first.

It has a commercial page structure problem first.

The highest-leverage issue is that multiple core commercial pages appear to expose `Contact Us` as the first `H1`, which weakens:

- SEO intent clarity
- AI extraction quality
- user trust
- conversion framing

Before scaling more pages, Anata should fix the live service-page architecture and the homepage/service-page heading structure.

## Inventory Snapshot

- total published pages: `22`
- total published posts: `140`
- current primary service tree:
  - `/services/`
  - `/services/fulfillment/`
  - `/services/shipping/`
  - `/services/ai/`
- legacy service tree still live:
  - `/ecommerce-services/advertising/`
  - `/ecommerce-services/web-design/`

## Core Findings

### P0. Critical heading mismatch on commercial pages

Observed live page title/H1 pairs:

- `/`
  - title: `anata inc. – Unleash your brands Amazon potential.`
  - first H1 found: `Contact Us`
- `/services/`
  - title: `Services – anata inc.`
  - first H1 found: `Contact Us`
- `/services/shipping/`
  - title: `Shipping – anata inc.`
  - first H1 found: `Contact Us`
- `/services/ai/`
  - title: `anata intelligence – anata inc.`
  - first H1 found: `Contact Us`
- `/services/fulfillment/`
  - title: `eCommerce 3PL Warehousing and Fulfillment – anata inc.`
  - first H1 found: `Ecomm Fulfillment.`

Implication:

- the live template or Elementor structure is likely putting a form/contact heading ahead of the real page heading
- homepage and service pages are not presenting a clean primary topic signal

### P0. Commercial architecture is split across current and legacy paths

Active service routes currently include:

- `/services/...`
- `/ecommerce-services/...`

Implication:

- mixed architecture
- diluted internal linking structure
- unclear canonical service system
- weak topical authority signaling

### P1. Commercial coverage does not match the blog/content footprint

Observed:

- substantial fulfillment/shipping post volume
- historical Amazon content volume
- limited current service-page depth for Amazon commercial intent

Implication:

- Anata has content support inventory, but not enough current commercial landing pages to capture decision-stage traffic well

### P1. Homepage positioning is carrying too many jobs

Observed previously and reinforced by title/heading mismatch:

- homepage appears to be handling too many offers and possibly multiple contact-oriented blocks

Implication:

- weak routing into service clusters
- weak commercial prioritization
- higher risk of generic “agency” feel

### P1. Legacy titles and quality issues remain in live footprint

Examples from live/public inventory:

- generic title patterns like `Services – anata inc.` and `Shipping – anata inc.`
- old service pages under `/ecommerce-services/`
- historical post inventory that may not align with the current service system

Implication:

- inconsistent SERP presentation
- lower click-through quality
- harder for AI systems to identify the best canonical service page per topic

## Priority Action Queue

## Phase 1: Fix the live commercial signal

### 1. Fix H1 structure on these pages first

- `/`
- `/services/`
- `/services/shipping/`
- `/services/ai/`
- `/services/fulfillment/`

Goal:

- one clear, service-relevant H1 per page
- no contact form heading or popup heading should appear as the page H1

### 2. Turn `/services/` into a real service hub

It should:

- introduce Anata's primary service families
- route into the highest-priority clusters
- not act like a generic contact or catch-all page

### 3. Decide the canonical commercial architecture

Recommendation:

- keep `/services/...` as the commercial standard
- treat `/ecommerce-services/...` as legacy
- redirect or retire legacy pages once replacements are ready

## Phase 2: Build the first real cluster

Recommendation:

- first commercial cluster: Amazon growth services

Suggested page set:

- `/services/amazon-ppc-management/`
- `/services/amazon-seo-services/`
- `/services/amazon-listing-optimization/`
- `/services/amazon-account-management/`

Support content can then link into those pages from the existing content base.

## Phase 3: Strengthen current fulfillment/shipping pages

Use the existing content footprint to improve these pages:

- `/services/fulfillment/`
- `/services/shipping/`

Required improvements:

- clearer H1/H2 structure
- stronger service definition
- operational process section
- FAQ layer
- stronger CTA path
- internal links from relevant posts

## 7-Day Execution Plan

### Day 1

- inspect the Elementor structure for homepage and core service pages
- identify why `Contact Us` is surfacing as the first H1
- fix H1 structure on homepage and `/services/`

### Day 2

- fix H1/title/hero structure on `/services/shipping/`
- fix H1/title/hero structure on `/services/ai/`
- improve `/services/fulfillment/` headline and above-the-fold clarity

### Day 3

- map legacy `/ecommerce-services/` pages against the new `/services/` system
- mark which should redirect, retire, or be rebuilt

### Day 4

- define the Amazon commercial cluster
- draft slugs, parent/child structure, and internal links

### Day 5

- audit the top fulfillment/shipping blog posts
- add internal links into the canonical service pages

### Day 6

- tighten homepage routing so it pushes users into 3 to 4 priority service families

### Day 7

- publish the first backlog report:
  - fixed pages
  - pages queued
  - pages to rebuild next

## Safe Working Rules Without Staging

- backup/export page content before major Elementor edits
- change one commercial page at a time
- avoid plugin changes during production optimization
- do heading and structure fixes before large visual redesigns

## Recommended Next Deliverable

The next deliverable should be:

`Anata Service Architecture Map`

That map should define:

- canonical service URLs
- legacy routes to retire
- first cluster to build
- internal linking rules between service pages and blog content
