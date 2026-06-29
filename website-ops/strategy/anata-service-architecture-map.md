# Anata Service Architecture Map

Date: 2026-03-25  
Status: production architecture recommendation  
Basis: live WordPress API inventory, live route/header inspection, current published service and post inventory

## Objective

Turn `anatainc.com` into a clean commercial architecture that:

- supports topical authority
- supports AI extraction
- avoids service-page cannibalization
- routes traffic into clear conversion paths
- can scale inside Elementor without creating page drift

## Current State

### Published service-related pages observed

Canonical-looking service pages:

- `https://anatainc.com/services/`
- `https://anatainc.com/services/fulfillment/`
- `https://anatainc.com/services/shipping/`
- `https://anatainc.com/services/ai/`

Legacy service pages still live:

- `https://anatainc.com/ecommerce-services/advertising/`
- `https://anatainc.com/ecommerce-services/web-design/`

Commercial/support pages outside the service tree:

- `https://anatainc.com/free-marketing-analysis/`
- `https://anatainc.com/contact/`
- `https://anatainc.com/amazon-profit-calculator/`
- `https://anatainc.com/ecommerce-fulfillment-toolkit-ebook/`

### Route behavior confirmed

Short routes already redirect correctly:

- `/ai` -> `/services/ai/`
- `/shipping` -> `/services/shipping/`
- `/fulfillment` -> `/services/fulfillment/`

This is good.

Legacy ecommerce service pages do **not** redirect:

- `/ecommerce-services/advertising/` returns `200`
- `/ecommerce-services/web-design/` returns `200`

This is the live architecture conflict that should be resolved.

## Commercial Architecture Rule

Use one commercial route pattern only:

```text
/services/{service-name}/
```

Use supporting route families for non-commercial intent:

```text
/insights/{article-name}/
/resources/{asset-name}/
/case-studies/{case-study-name}/
/contact/
```

Do not publish new money pages outside `/services/`.

## Canonical Route Map

### Canonical service hub

```text
/services/
```

Job:

- service directory
- cluster entry point
- high-level routing page

It should not behave like a contact page or generic brochure page.

### Canonical service families

#### 1. Amazon growth

Recommended URLs:

```text
/services/amazon-marketing-services/
/services/amazon-ppc-management/
/services/amazon-seo-services/
/services/amazon-listing-optimization/
/services/amazon-account-management/
/services/amazon-brand-registry-ip-protection/
```

Reason:

- strongest existing topical support in the blog inventory
- clearest decision-stage commercial opportunity
- strongest fit for Anata's positioning

#### 2. Fulfillment

Current canonical:

```text
/services/fulfillment/
```

Recommended supporting services if expanded later:

```text
/services/fulfillment-software/
/services/3pl-warehousing/
/services/pick-pack-fulfillment/
```

#### 3. Shipping software / shipping operations

Current canonical:

```text
/services/shipping/
```

Recommended future refinement:

```text
/services/shipping-software/
```

If the current offer is software-led rather than generic shipping service, the slug should eventually reflect that more clearly.

#### 4. AI / automation

Current canonical:

```text
/services/ai/
```

Recommended future refinement:

```text
/services/ai-automation/
```

Reason:

- `ai` is broad and weak as a decision-stage service URL
- `ai-automation` or another explicit service label is more commercially legible

This is not a first-week change, but it should be considered.

#### 5. Shopify growth

Recommended future URLs if this line remains strategic:

```text
/services/shopify-growth-services/
/services/shopify-conversion-optimization/
```

#### 6. TikTok Shop

Recommended future URLs if this line remains strategic:

```text
/services/tiktok-shop-management/
/services/tiktok-shop-ads/
```

## Legacy Route Handling

### Keep

Keep:

- `/services/`
- `/services/fulfillment/`
- `/services/shipping/`
- `/services/ai/`

Keep the short-path redirects already in place:

- `/fulfillment`
- `/shipping`
- `/ai`

### Retire or replace

Legacy URLs that should not remain as active strategic service pages:

- `/ecommerce-services/advertising/`
- `/ecommerce-services/web-design/`

Decision rule:

- if the service is still strategic, rebuild it under `/services/...` first, then 301 redirect the legacy URL
- if the service is not strategic, retire the page and redirect to the best-fit parent page or `/services/`

Current recommendation:

- `advertising` should be absorbed into the Amazon growth cluster, not kept as a generic standalone legacy page
- `web-design` is likely off-strategy for the current Anata positioning and should not be a priority commercial page unless it still drives revenue

## Homepage Routing Rule

Homepage job:

- establish category-level positioning
- route users to 3 to 4 core service families
- push high-intent visitors toward the strongest service pages

Homepage should not try to be the full explanation page for every offer.

Recommended homepage route targets:

- `/services/amazon-marketing-services/`
- `/services/fulfillment/`
- `/services/shipping/`
- `/services/ai/`

Secondary actions:

- `/free-marketing-analysis/`
- `/contact/`

## Service Hub Rule

`/services/` should become the controlled service directory.

Required content blocks:

1. brief definition of Anata's operating model
2. service family cards
3. short service comparison/selection guidance
4. proof/trust block
5. CTA

Required outbound links from `/services/`:

- every active strategic service family
- no dead or legacy service pages

## Cluster Order

Build in this order:

1. Amazon growth
2. Fulfillment
3. Shipping software
4. AI / automation
5. Shopify growth
6. TikTok Shop

## First Cluster: Amazon Growth

### Why this cluster first

- large existing Amazon content footprint
- stronger commercial intent than the legacy generic advertising page
- best opportunity to convert historical topical relevance into money pages

### Cluster map

Hub:

```text
/services/amazon-marketing-services/
```

Core money pages:

```text
/services/amazon-ppc-management/
/services/amazon-seo-services/
/services/amazon-listing-optimization/
/services/amazon-account-management/
```

Optional specialist page:

```text
/services/amazon-brand-registry-ip-protection/
```

Support content to create or refresh:

```text
/insights/amazon-acos-vs-tacos/
/insights/how-amazon-ppc-management-works/
/insights/what-amazon-listing-optimization-includes/
```

Existing posts that can support this cluster already exist in the blog inventory around:

- Amazon PPC
- Amazon listing optimization
- Amazon SEO
- Amazon advertising
- Amazon account operations

## Fulfillment Cluster

Current commercial anchor:

```text
/services/fulfillment/
```

Observed blog support is already strong in:

- 3PL
- pick and pack
- warehouse automation
- fulfillment software
- operational efficiency

This means the fulfillment cluster can become strong quickly once the main service page is structurally improved and better linked.

## Shipping Cluster

Current commercial anchor:

```text
/services/shipping/
```

Required decision:

- is this a service page for shipping operations
- or a product page for shipping software

The page naming and route should reflect the actual commercial offer. Right now `shipping` is too generic.

## AI Cluster

Current commercial anchor:

```text
/services/ai/
```

Required decision:

- what exact business outcome does this page sell
- where does it fit relative to Anata's ecommerce core

Without that clarity, it risks being too broad to rank or convert well.

## Internal Linking Rules

Every active service page must:

- receive at least 2 internal links from relevant pages
- link to at least 2 sibling or supporting pages
- include one CTA path to contact or analysis

Linking pattern:

- homepage -> service family pages
- service hub -> all strategic service pages
- support posts -> corresponding service pages
- service pages -> support posts and adjacent services
- calculators/resources -> best-fit money pages

## Title And Heading Rules

Every commercial page must have:

- one clear service-relevant `H1`
- a title tag aligned to the target service intent
- no `Contact Us` heading surfacing as the first `H1`

This is currently violated on multiple core pages and should be fixed before scaling more service pages.

## Page-Type Rules

### Service pages

Use for:

- decision-stage commercial intent
- lead generation
- clear offer definition

### Insights posts

Use for:

- question-based search intent
- educational support
- AI answer visibility

### Resources

Use for:

- tools
- calculators
- downloadable assets

## Redirect Plan

### Keep active redirects

- `/ai` -> `/services/ai/`
- `/shipping` -> `/services/shipping/`
- `/fulfillment` -> `/services/fulfillment/`

### Add after replacement pages exist

- `/ecommerce-services/advertising/` -> best-fit new Amazon service page or `/services/amazon-marketing-services/`
- `/ecommerce-services/web-design/` -> `/services/` or another current best-fit destination

Do not redirect legacy URLs until replacement pages are live and internal links have been updated.

## First 10 Execution Moves

1. Fix homepage H1 structure.
2. Fix `/services/` H1 structure and routing logic.
3. Fix `/services/shipping/` H1 structure.
4. Fix `/services/ai/` H1 structure.
5. Improve `/services/fulfillment/` hero and service definition.
6. Build `/services/amazon-marketing-services/`.
7. Build `/services/amazon-ppc-management/`.
8. Build `/services/amazon-seo-services/`.
9. Build `/services/amazon-listing-optimization/`.
10. Decide retirement plan for legacy `/ecommerce-services/` pages.

## Decision Log

### Confirmed decisions

- `/services/...` is the canonical commercial route family
- short aliases like `/shipping` should remain redirects, not separate indexable pages
- legacy `/ecommerce-services/...` pages should not remain part of the long-term service architecture

### Open decisions

- whether `shipping` should be renamed to `shipping-software`
- whether `ai` should be renamed to a more explicit commercial slug
- whether `web-design` is still a real revenue service

## Definition Of Good Architecture

The Anata service architecture is healthy when:

- all commercial pages live under `/services/`
- homepage routes to the core service families cleanly
- no legacy service tree remains live without purpose
- every service cluster has a hub plus supporting pages
- service-page titles and H1s clearly match commercial intent
