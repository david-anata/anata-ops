# Anata Website Ops Audit Walkthrough

Date: 2026-03-26
Scope: live production audit, recommendation pass, and direct implementation verification

## What We Audited

Monitored URLs:

- `https://anatainc.com/`
- `https://anatainc.com/services/`
- `https://anatainc.com/services/fulfillment/`
- `https://anatainc.com/services/shipping/`
- `https://anatainc.com/services/ai/`
- `https://anatainc.com/services/advertising/`
- `https://anatainc.com/contact/`

## What Was Already Fixed Before This Pass

These pages were previously corrected and are now healthy in the live audit:

- homepage H1
- services hub H1
- shipping page H1
- AI page H1

Those earlier fixes removed generic popup or CTA-style H1 conflicts and restored page-topic headings as the primary H1.

## Fresh Audit Findings Before Implementation

### 1. Fulfillment Page

URL: `https://anatainc.com/services/fulfillment/`

Problem:

- the page exposed two H1s

Observed H1s:

- `Ecomm Fulfillment.`
- `Pricing that's Competitive and Scalable`

Recommendation:

- keep `Ecomm Fulfillment.` as the page H1
- demote the pricing block to H2

### 2. Contact Page

URL: `https://anatainc.com/contact/`

Problem:

- the page exposed no H1

Observed top heading:

- `Get connected with an expert today.` was present as H2

Recommendation:

- promote the contact hero heading to H1

### 3. Advertising Route Architecture

URL checked: `https://anatainc.com/services/advertising/`

Observed behavior:

- the route returns a 301 redirect to `https://anatainc.com/ecommerce-services/advertising/`
- canonical resolves to the legacy `/ecommerce-services/advertising/` URL

Recommendation:

- decide whether advertising should live under `/services/advertising/` or remain under `/ecommerce-services/advertising/`
- once decided, make the canonical route match the intended information architecture

This is not a broken page-level issue, but it is still a structural SEO and site-architecture issue.

## What We Implemented In This Pass

### Fulfillment Page

Implemented:

- changed the pricing block from H1 to H2

Affected page:

- WordPress page ID `2640`

### Contact Page

Implemented:

- changed `Get connected with an expert today.` from H2 to H1

Affected page:

- WordPress page ID `588`

## Backup Artifacts

Backups were written to:

- `website-ops/backups/2026-03-26-structural-fixes/`

Included artifacts:

- pre-change page records
- post-change page records
- content-level post-fix records

## Verification Results After Implementation

### Current Live Status

- `https://anatainc.com/` — healthy
- `https://anatainc.com/services/` — healthy
- `https://anatainc.com/services/fulfillment/` — healthy
- `https://anatainc.com/services/shipping/` — healthy
- `https://anatainc.com/services/ai/` — healthy
- `https://anatainc.com/services/advertising/` — healthy page response, but still redirects to legacy route
- `https://anatainc.com/contact/` — healthy

### Verified Heading State

Homepage:

- H1: `Ecommerce Accelerator Partner.`

Services hub:

- H1: `Our Ecommerce Services.`

Fulfillment:

- H1: `Ecomm Fulfillment.`

Shipping:

- H1: `Faster, Smarter, Stress-free Shipping.`

AI:

- H1: `Faster, Smarter, Intelligent, Data.`

Advertising:

- H1: `Advertising.`

Contact:

- H1: `Get connected with an expert today.`

## Final Summary

### Fixed in this session

- fulfillment multiple-H1 issue
- contact missing-H1 issue

### Confirmed healthy after verification

- homepage
- services hub
- fulfillment
- shipping
- AI
- contact

### Still needs a strategic decision

- advertising route structure and canonical path alignment

## Next Recommended Actions

1. standardize the advertising page under one canonical route family
2. expand the monitored URL set beyond the current seven pages
3. begin weekly queue-based improvements through the website-ops dashboard
