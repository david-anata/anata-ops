# Anata Website Implementation Guide

## Purpose

This is the Anata-specific execution guide for turning `anatainc.com` into a fast-moving SEO and AI-visibility system.

It is tailored to the current live site structure observed on:

- `https://anatainc.com/`
- `https://anatainc.com/services/fulfillment/`
- `https://anatainc.com/services/shipping/`
- `https://anatainc.com/fulfillment/`
- `https://anatainc.com/shipping/`

This guide assumes:

- WordPress is the CMS
- Elementor is the page builder
- the live site is `anatainc.com`
- staging does not exist yet
- Ahrefs and Semrush are not yet active

Where the actual host or DNS provider is unknown, follow the provider-specific branch that matches your stack.

---

## What The Live Site Indicates Right Now

### Current strengths

- The brand already has multiple service categories: Amazon, Shopify, TikTok Shop, fulfillment, and Shipping OS.
- The site already uses a `/services/` path for at least some money pages.
- There is a blog link, which means content clustering can be built without rethinking the whole site.
- The homepage already includes FAQs and lead forms, which can be tightened rather than invented from scratch.

### Current structural problems

- The homepage is trying to sell too many things at once.
- There are overlapping route patterns:
  - `/fulfillment/` and `/services/fulfillment/`
  - `/shipping/` and `/services/shipping/`
- This creates a real risk of duplicate intent, internal cannibalization, and weak canonical clarity.
- Several sections read like mixed templates rather than a controlled service architecture.
- There are visible quality issues on the live site, including spelling and formatting problems, which lowers trust and hurts conversion.

### What this means operationally

Anata should not add more pages randomly until the architecture is cleaned up.

The first goal is:

`standardize structure -> create staging -> lock tracking -> clean service architecture -> scale cluster production`

---

## Phase 1: Set Up The Stack Properly

### 1. Identify your host and DNS provider

Before touching the site, identify:

- WordPress host
- DNS provider
- CDN or proxy layer
- email/form provider

Most likely possibilities:

- host: WP Engine, Kinsta, SiteGround, Bluehost, Cloudways, Hostinger, or similar
- DNS: Cloudflare or the domain registrar

How to check:

1. Log into the domain registrar account.
2. Check where the nameservers point.
3. Log into the hosting account and verify the WordPress install tied to `anatainc.com`.
4. Confirm whether the host already offers one-click staging.

Document:

```text
Host:
DNS:
CDN/Proxy:
WordPress admin URL:
Primary admin account:
SEO plugin:
Caching/performance plugin:
Form plugin:
```

### 2. Create staging before further structural changes

Target:

```text
staging.anatainc.com
```

#### If your host has one-click staging

Use it.

Then:

1. Clone production to staging.
2. Password protect staging.
3. Set staging to `noindex`.
4. Confirm forms on staging do not pollute production leads.

#### If your host does not have one-click staging

Do this manually:

1. Create subdomain `staging.anatainc.com` in DNS.
2. Create a new site/app/server target for the subdomain.
3. Clone the live WordPress database and files.
4. Update the site URL in WordPress to the staging domain.
5. Password protect the entire site at the server or plugin level.
6. Set staging pages to `noindex`.

### 3. Lock staging index control correctly

Do all three:

- password protect staging
- enable `noindex`
- keep staging out of XML sitemaps if possible

Do not rely only on `robots.txt`.

---

## Phase 2: Clean The Information Architecture

### 4. Choose a single service URL pattern

Anata already appears to use:

```text
/services/{service-name}/
```

Make that the standard for all commercial service pages.

Recommended standard:

```text
/services/amazon-ppc-management/
/services/amazon-seo-services/
/services/amazon-listing-optimization/
/services/amazon-account-management/
/services/tiktok-shop-management/
/services/shopify-growth-services/
/services/fulfillment/
/services/shipping/
```

### 5. Resolve overlapping page paths

Current public overlap suggests at least these conflicts:

```text
/fulfillment/ vs /services/fulfillment/
/shipping/ vs /services/shipping/
```

Action:

1. Decide which URL is canonical.
2. Keep the stronger service architecture under `/services/`.
3. 301 redirect the non-canonical page to the canonical one.
4. Update all internal links to the canonical URL only.
5. Confirm canonicals match the chosen destination.

Recommendation:

- Keep `/services/fulfillment/`
- Keep `/services/shipping/`
- Redirect `/fulfillment/` and `/shipping/`

### 6. Separate site sections by intent

Recommended top-level structure:

```text
/services/
/industries/
/insights/
/case-studies/
/resources/
/contact/
```

Use each section for a different job:

- `/services/` for buyer-ready commercial pages
- `/insights/` for educational support content
- `/case-studies/` for proof
- `/resources/` for calculators, audits, ebooks, tools

Do not use the homepage as the dumping ground for every offer.

---

## Phase 3: Define Anata's Commercial Clusters

### 7. Do not scale every line of business at once

The homepage currently spans:

- Amazon services
- TikTok Shop services
- Shopify services
- fulfillment
- shipping software

That is too wide for a fast SEO system unless clusters are controlled.

Recommended order of execution:

1. Amazon growth cluster
2. Fulfillment cluster
3. Shipping OS cluster
4. TikTok Shop cluster
5. Shopify growth cluster

### 8. First cluster to operationalize

Start with the Amazon cluster because:

- it is clearly central to the brand
- the homepage already emphasizes Amazon heavily
- the service intent is easier to structure into commercial pages
- the query space is large enough to support fast cluster growth

Recommended first cluster:

```text
Hub or overview:
/services/amazon-marketing-services/

Core pages:
/services/amazon-ppc-management/
/services/amazon-seo-services/
/services/amazon-listing-optimization/
/services/amazon-account-management/
/services/amazon-brand-registry-ip-protection/

Support pages:
/insights/amazon-acos-vs-tacos/
/insights/how-amazon-ppc-management-works/
/insights/what-amazon-listing-optimization-includes/
```

### 9. Fulfillment and Shipping should remain separate product families

Do not collapse these into the Amazon cluster.

Use separate messaging:

- fulfillment = operational logistics service
- shipping = software/platform product

That distinction matters for search intent and conversion.

---

## Phase 4: WordPress and Elementor Controls

### 10. Standardize Elementor templates immediately

Build these reusable templates in Elementor:

- service hero
- problem grid
- outcomes grid
- process steps
- scope/included checklist
- FAQ accordion
- proof/testimonial strip
- final CTA block

Why:

- current live pages feel inconsistent
- template standardization is what makes scale possible

### 11. Create one global service-page template spec

Every service page should follow:

1. Hero
2. What this is
3. Problems we solve
4. Outcomes
5. How it works
6. What's included
7. Why Anata
8. FAQ
9. CTA

Inside Elementor, map this to standard widgets only:

- heading
- text editor
- icon box
- icon list
- button
- accordion
- image
- form
- testimonial carousel only if lightweight

### 12. Keep the plugin stack controlled

Verify and document:

- SEO plugin
- cache/performance plugin
- redirect management tool
- schema source
- form plugin

Rules:

- one SEO plugin only
- one redirect manager only
- avoid duplicate schema plugins
- avoid multiple optimization plugins fighting each other

---

## Phase 5: Tracking Setup For Anata

### 13. Configure Search Console around the real domain

Create:

- `anatainc.com` domain property
- `https://anatainc.com/` URL-prefix property

Then:

1. verify by DNS
2. submit sitemap
3. inspect homepage
4. inspect one service page
5. inspect one blog page

### 14. Configure GA4 for lead visibility

Set up production GA4 with these events:

- `generate_lead`
- `form_submit`
- `click_call`
- `click_email`
- `book_demo`
- `book_audit`

Mark these as conversions where appropriate:

- form submission
- booked audit/demo
- qualified lead actions

### 15. Set up Bing Webmaster Tools

Use the same sitemap.

Even if traffic is smaller, Bing visibility still matters for discovery and AI downstream surfaces.

---

## Phase 6: Performance Work For The Existing Site

### 16. Run a homepage cleanup first

The current homepage likely has too many sections and too many mixed offers.

Immediate homepage fixes:

1. Tighten the hero to a single primary commercial statement.
2. Reduce stacked offer blocks.
3. Remove low-quality filler or duplicated sections.
4. Fix visible spelling and formatting errors.
5. Keep one primary CTA and one secondary CTA.
6. Make the homepage route people into defined service clusters, not every offer equally.

### 17. Clean quality issues before scaling

Examples visible on the live site include:

- `Consistancy`
- `Optimizatons`
- `anaytics`
- `EVERDAY USERS`

These are not minor.

They damage:

- trust
- conversion
- perceived expertise
- AI extractability

Before expanding volume, clean obvious page-quality issues on high-traffic pages.

### 18. Set Core Web Vitals priorities

Start with:

- homepage
- top 3 service pages
- contact page

Focus on:

- image compression
- widget reduction
- mobile spacing cleanup
- minimizing heavy animations
- limiting font payload

---

## Phase 7: Content And SEO Production System For Anata

### 19. Create the page production tracker

Use Google Sheets or Airtable.

Columns:

```text
URL
Page type
Cluster
Primary keyword
Secondary keywords
Status
Owner
Publish date
Last updated
Indexed
Clicks
Impressions
CTR
Avg position
Leads
Primary CTA
Next action
```

### 20. Publish in clusters, not randomly

Required publishing rule:

- every new service page must launch with at least 2 internal links pointing to it
- every new service page must link out to at least 2 related pages
- every new service page must include FAQ and AI extraction blocks

### 21. Use this page priority order

Priority 1:

- homepage
- `/services/amazon-ppc-management/`
- `/services/amazon-seo-services/`
- `/services/amazon-listing-optimization/`

Priority 2:

- `/services/amazon-account-management/`
- `/services/amazon-brand-registry-ip-protection/`
- first 3 support articles in `/insights/`

Priority 3:

- fulfillment cluster cleanup
- shipping cluster cleanup

---

## Phase 8: SEO Tooling Decision Path

### 22. If you buy only one SEO platform now

Choose `Ahrefs` first.

Reason:

- better fit for backlink discovery, keyword gaps, SERP checks, and competitor page comparison for service-page growth

Buy `Semrush` only if you need its broader marketing suite more than pure SEO workflow.

### 23. If budget is not ready yet

Operate with:

- Google Search Console
- GA4
- Bing Webmaster Tools
- PageSpeed Insights
- Google Sheets

This is enough to begin ranking work.

### 24. Add Screaming Frog once page count grows

Use it for:

- crawl status
- canonical conflicts
- duplicate titles
- missing H1s
- broken links
- orphan pages
- oversized assets

---

## Phase 9: Agents And Cron Workflows

### 25. The minimum autonomous system

Set up recurring jobs for:

- daily site health checks
- weekly ranking and landing-page reviews
- weekly refresh queue generation
- monthly technical SEO review

### 26. Weekly report structure

Every weekly report should answer:

1. Which pages gained visibility?
2. Which pages lost clicks or CTR?
3. Which pages are ranking but under-converting?
4. Which pages need refresh first?
5. Which new cluster pages should be built next?

### 27. Recommended automation cadence

```text
Daily 07:00 America/Denver
- homepage uptime check
- service page status checks
- 404/redirect spot checks

Weekly Monday 08:00 America/Denver
- Search Console export
- GA4 landing page review
- page refresh recommendations

Monthly 1st day 09:00 America/Denver
- architecture audit
- Core Web Vitals review
- internal linking gap review
```

### 28. Suggested folders for reports

```text
/website-ops/reports/daily/
/website-ops/reports/weekly/
/website-ops/reports/monthly/
```

Each report should include:

- date
- pages reviewed
- issues found
- recommended fixes
- priority
- status

---

## Phase 10: Immediate Actions For Anata

### First 48 hours

1. Identify host, DNS, and current WordPress plugin stack.
2. Create `staging.anatainc.com`.
3. Password protect staging and add `noindex`.
4. Set up GSC domain property and sitemap submission.
5. Set up GA4 events and conversions.
6. Decide canonical URLs for fulfillment and shipping pages.
7. Fix visible homepage typos and low-trust formatting issues.

### First 7 days

1. Create Elementor templates for service pages.
2. Rebuild the first 3 Amazon service pages using the standardized structure.
3. Add internal links and FAQ blocks.
4. Publish the first 3 support articles in `/insights/`.
5. Review indexing and page quality.

### First 30 days

1. Finish the first Amazon service cluster.
2. Build the weekly reporting workflow.
3. Create the refresh queue.
4. Start fulfillment/shipping architecture cleanup.
5. Purchase Ahrefs if budget allows.

---

## Decision Branches By Hosting Stack

### If the host is WP Engine

- use WP Engine staging
- protect staging with password protection
- use WP Engine redirects if needed, or your redirect plugin
- test cache behavior after template changes

### If the host is Kinsta

- create staging in MyKinsta
- protect staging with password or IP restriction
- use Kinsta cache controls after page edits

### If the host is SiteGround

- use SiteGround staging if available
- check SG Optimizer settings carefully so optimization does not break Elementor

### If the host is Cloudways

- create a staging app or clone app
- confirm domain mapping and SSL on staging only if needed

### If DNS is Cloudflare

- create the `staging` DNS record there
- use basic auth or application-level protection
- keep CDN/caching rules documented

---

## Definition Of Done

Anata is ready for autonomous website operations when:

- staging exists
- staging is password protected and `noindex`
- GSC, GA4, and Bing are live
- URL architecture is standardized
- overlapping service routes are resolved
- Elementor templates are standardized
- the first Amazon cluster is live
- weekly reports are running
- monthly refresh reviews are running

---

## Official References

- Google AI features and your site  
  https://developers.google.com/search/docs/appearance/ai-features
- Google Helpful content guidance  
  https://developers.google.com/search/docs/fundamentals/creating-helpful-content
- Google SEO Starter Guide  
  https://developers.google.com/search/docs/fundamentals/seo-starter-guide
- Google block indexing with `noindex`  
  https://developers.google.com/search/docs/crawling-indexing/block-indexing
- Google robots.txt guidance  
  https://developers.google.com/search/docs/crawling-indexing/robots/intro
- Google sitemap guidance  
  https://developers.google.com/search/docs/crawling-indexing/sitemaps/build-sitemap
- Google Search Console property setup  
  https://support.google.com/webmasters/answer/34592
- Google Analytics 4 website setup  
  https://support.google.com/analytics/answer/9306384
