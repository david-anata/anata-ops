# Anata Website Production Setup

## Objective

Build a website operating system for Anata that:

- publishes service pages fast
- ranks in Google
- earns inclusion in AI-generated answers
- improves conversion rate over time
- tracks changes and performance continuously
- can be operated inside WordPress + Elementor without chaos

This setup is designed for speed first, then scale, then iteration.

## Core Rule

Treat the website like an operating system:

`research -> build -> publish -> measure -> improve -> repeat`

Do not treat pages as one-off content projects.

---

## Phase 1: Foundation

### 1. Set up a staging site

Use a staging subdomain:

```text
staging.anatainc.com
```

Preferred order:

1. Use your host's built-in staging feature if available.
2. If the host has no staging feature, create a subdomain manually and clone the site into it.
3. Keep the same theme, Elementor version, plugins, and permalink structure as production.

Staging requirements:

- password protect the staging site
- add `noindex` to all staging pages
- do not rely on `robots.txt` alone to keep staging out of Google
- disable production conversion tracking on staging or use a separate GA4 property

Why this matters:

- Google explicitly states that `robots.txt` is not a reliable way to keep pages out of search
- Google recommends `noindex` or password protection for non-public pages

Elementor-specific checks on staging:

- global fonts and colors match production
- header/footer templates render correctly
- forms submit correctly
- responsive breakpoints are intact
- reusable section templates are available

### 2. Set up access and ownership

Create or verify access for:

- WordPress admin
- hosting dashboard
- domain DNS
- Google Search Console
- Google Analytics 4
- Google Tag Manager if used
- Bing Webmaster Tools
- Cloudflare if used

Use one shared operator account for ownership recovery and one named user account for daily work.

### 3. Set up Google Search Console correctly

Create:

- one `Domain property` for the full root domain
- one `URL-prefix property` for the live canonical site if helpful for debugging

Then:

- verify by DNS
- submit the sitemap
- inspect the homepage
- inspect one core service page after publishing

Target properties:

```text
example: anatainc.com
example: https://anatainc.com/
```

### 4. Set up GA4 correctly

Create a GA4 property for the production site.

Recommended events:

- `generate_lead`
- `form_submit`
- `click_call`
- `click_email`
- `book_consultation`

Recommended conversions:

- primary form submission
- booked call
- qualified contact action

If staging is tracked at all, use a separate property or separate data stream.

### 5. Set up Bing Webmaster Tools

Do this even if Google is the main priority.

Reasons:

- Bing powers some downstream AI and assistant experiences
- indexing feedback is useful
- sitemap submission is easy

---

## Phase 2: WordPress and Elementor Stack

### 6. Keep the plugin stack lean

Recommended baseline:

- Elementor
- Elementor Pro
- one SEO plugin only: `Rank Math` or `Yoast`
- one redirects plugin if not included elsewhere
- one performance plugin if needed
- one snippets/code manager plugin if custom code is required

Avoid stacking overlapping plugins for:

- schema
- redirection
- image optimization
- caching
- analytics injection

Too many plugins create conflicting output and slower pages.

### 7. Standardize global templates

Inside Elementor, create reusable templates for:

- hero
- problem/outcome grid
- process steps
- FAQ accordion
- CTA strip
- proof/testimonial block

Goal:

- no service page should start from a blank canvas

### 8. Standardize site settings

Lock these before scale:

- typography system
- spacing scale
- button styles
- heading hierarchy
- container widths
- mobile spacing rules
- image aspect ratios

This prevents page-by-page drift.

---

## Phase 3: SEO Infrastructure

### 9. Configure technical SEO basics

Make sure production has:

- one canonical version of the domain
- valid sitemap
- correct indexability settings
- clean title and meta control
- open graph defaults
- schema support
- redirect management
- 404 handling

Check these manually:

- homepage canonical
- service page canonical
- trailing slash consistency
- `www` vs non-`www`
- HTTP to HTTPS redirect

### 10. Create a URL architecture before publishing at scale

Recommended service structure:

```text
/services/
/services/amazon-ppc-management/
/services/amazon-seo-services/
/services/amazon-listing-optimization/
```

Recommended supporting content structure:

```text
/insights/
/insights/amazon-ppc-acos-vs-tacos/
/insights/how-to-improve-amazon-conversion-rate/
```

Rules:

- short descriptive slugs
- one keyword theme per page
- no duplicate service pages for the same intent
- parent/child structure should reflect actual clusters

### 11. Create the sitemap and submit it

On WordPress, your SEO plugin will usually generate the sitemap automatically.

After setup:

- confirm the sitemap loads in browser
- confirm service pages appear in it
- submit it to Google Search Console
- submit it to Bing Webmaster Tools

### 12. Use FAQ and schema carefully

Use FAQs because they help users and AI extraction.

Do not use FAQs as a shortcut or filler.

Rules:

- questions must be relevant to the service intent
- answers must be short and direct
- on-page FAQ content must match any schema markup
- do not add schema for content that is not visibly present

---

## Phase 4: Performance Setup

### 13. Establish Core Web Vitals targets

Production targets:

```text
LCP < 2.5s
INP < 200ms
CLS < 0.1
```

Priority pages:

- homepage
- all money pages
- top organic landing pages

### 14. Performance checklist for Elementor pages

Before publish:

- compress hero images
- avoid oversized background videos
- keep above-the-fold layout simple
- reduce unused widgets
- avoid animation overload
- limit font families and weights
- lazy load below-the-fold images
- test mobile separately

Recommended test tools:

- PageSpeed Insights
- Chrome Lighthouse
- Search Console Core Web Vitals report

---

## Phase 5: Content Production System

### 15. Define the page production template

Every service page must include:

1. Hero
2. What this is
3. Problems we solve
4. Outcomes
5. How it works
6. What's included
7. Why Anata
8. FAQ
9. CTA

Each page also needs:

- primary keyword theme
- supporting keyword cluster
- internal links
- AI extraction block
- conversion CTA

### 16. Build the first cluster, not random pages

Start with one commercial cluster only.

Example cluster:

```text
Hub:
- Amazon Marketing Services

Core service pages:
- Amazon PPC Management
- Amazon SEO Services
- Amazon Listing Optimization
- Amazon DSP Management

Support pages:
- Amazon ACoS vs TACoS
- How Amazon PPC Campaign Structure Works
- What Amazon Listing Optimization Includes
```

Do not spread effort across unrelated services until one cluster is live and internally linked.

### 17. Track every page in a production sheet

Create a sheet with these columns:

```text
Page URL
Service
Primary keyword
Cluster
Stage
Publish date
Last updated
Primary CTA
Internal links added
Schema added
Indexed
Clicks
Impressions
Average position
Leads
Next action
```

Stages:

```text
Backlog
Drafting
In Build
In QA
Published
Indexed
Improving
```

---

## Phase 6: Tracking and Iteration

### 18. Define the weekly SEO review loop

Every week:

- review new pages for indexing
- review top landing pages in GSC
- review queries with impressions but weak CTR
- review pages ranking between positions 5 and 20
- refresh internal links
- improve weak FAQ answers
- tighten CTAs on high-traffic pages

### 19. Define the monthly optimization loop

Every month:

- identify pages with high impressions and low clicks
- identify pages with clicks but weak lead rate
- expand thin service pages with missing decision-stage detail
- add supporting cluster pages
- update outdated stats, process steps, and proof points
- compare page templates against actual conversion behavior

### 20. Use a simple page scoring model

Score each page 1 to 5 across:

- intent match
- topic coverage
- internal linking
- conversion clarity
- trust/proof
- AI extraction quality
- mobile usability

Pages scoring under `4` should enter the refresh queue.

---

## Phase 7: Agents, Crons, and Self-Improvement

### 21. Operating model

Use three system layers:

1. `Build layer`
   - create and update service page briefs
   - generate section structure
   - maintain internal-link suggestions

2. `Monitor layer`
   - pull GSC and GA4 data
   - detect indexing gaps
   - detect pages losing clicks or CTR
   - detect pages with traffic but weak lead rate

3. `Improve layer`
   - queue pages for refresh
   - propose FAQ updates
   - propose new support pages
   - propose CTA and trust-element fixes

### 22. Recommended automations

Set up these recurring jobs:

#### Daily

- check uptime
- check new 404s or redirect errors
- check newly published pages for indexability

#### Weekly

- export GSC page/query performance
- export GA4 landing page + conversion performance
- generate winners/losers report
- identify pages needing refresh
- identify internal linking opportunities

#### Monthly

- cluster coverage audit
- technical SEO audit
- page speed review of money pages
- service page refresh queue generation

### 23. Suggested cron job table

```text
Daily 07:00 - uptime + indexability + 404 check
Weekly Mon 08:00 - GSC export and service page ranking review
Weekly Mon 08:30 - GA4 landing page conversion review
Weekly Mon 09:00 - refresh queue generation
Monthly 1st 09:00 - cluster gap and technical SEO audit
```

### 24. Data outputs each automation should produce

Store outputs in a shared location:

```text
/reports/daily/
/reports/weekly/
/reports/monthly/
```

Each report should include:

- date
- pages checked
- pages with issues
- changes recommended
- priority
- owner

### 25. Minimum automation stack

If budget is tight:

- Google Search Console
- GA4
- Bing Webmaster Tools
- PageSpeed Insights
- Google Sheets or Airtable
- Codex automations / local cron jobs

If budget expands later:

- Ahrefs or Semrush for SERP and gap tracking
- Screaming Frog for scheduled crawls
- Looker Studio dashboard for executive reporting

---

## Phase 8: Recommended Tool Decisions

### 26. SEO platform choice

If buying one SEO tool, choose in this order:

1. `Ahrefs` if priority is backlinks, content gaps, and competitor visibility
2. `Semrush` if priority is broader marketing reporting and workflow breadth

For Anata's current stage, Ahrefs is likely the cleaner first buy if the goal is service-page competition and topic-gap analysis.

### 27. Crawler choice

Use `Screaming Frog` once there are enough pages to justify recurring crawls.

Use it for:

- titles
- meta descriptions
- canonicals
- headings
- indexability
- broken links
- image size
- orphan pages

### 28. Dashboard choice

Use `Looker Studio` once GSC and GA4 are live.

Create views for:

- service page performance
- landing page conversion
- query growth
- cluster coverage
- refresh queue

---

## Phase 9: Definition of Done

The website is operational when all of the following are true:

- staging exists and is protected from indexing
- production has GSC, GA4, and Bing configured
- sitemap is submitted
- one cluster is fully mapped
- reusable Elementor templates exist
- page production sheet exists
- weekly review cadence exists
- monthly refresh cadence exists
- automation outputs are being stored and reviewed

---

## Immediate Next Steps

### 30-minute setup

1. Create `staging.anatainc.com`
2. Password protect it
3. Add `noindex`
4. Create GSC domain property
5. Create GA4 property
6. Submit sitemap
7. Confirm one service page template inside Elementor

### First week

1. Publish or rebuild the first 3 service pages in one cluster
2. Add internal links between them
3. Add FAQ and AI extraction blocks
4. Track them in the production sheet
5. Inspect them in Search Console

### First month

1. Build one complete cluster
2. Set up weekly and monthly reporting
3. Identify pages with the most leverage
4. Start refresh cycles instead of only adding pages

---

## Notes for Anata

Anata should compete by being:

- clearer than generic agencies
- more operational than copy-heavy SEO firms
- better structured for both search engines and AI systems
- faster at publishing clustered service pages than competitors

The edge is not "better writing."

The edge is:

- better page structure
- better topical coverage
- better update cadence
- better internal linking
- better performance tracking
- faster iteration

---

## Official References

- Google Search Central: AI features and your site  
  https://developers.google.com/search/docs/appearance/ai-features
- Google Search Central: Helpful content  
  https://developers.google.com/search/docs/fundamentals/creating-helpful-content
- Google Search Central: SEO Starter Guide  
  https://developers.google.com/search/docs/fundamentals/seo-starter-guide
- Google Search Central: Block indexing with `noindex`  
  https://developers.google.com/search/docs/crawling-indexing/block-indexing
- Google Search Central: robots.txt intro  
  https://developers.google.com/search/docs/crawling-indexing/robots/intro
- Google Search Central: Sitemaps overview  
  https://developers.google.com/search/docs/crawling-indexing/sitemaps/overview
- Google Search Central: Build and submit a sitemap  
  https://developers.google.com/search/docs/crawling-indexing/sitemaps/build-sitemap
- Google Search Central: Core Web Vitals  
  https://developers.google.com/search/docs/appearance/core-web-vitals
- Search Console Help: Add a website property  
  https://support.google.com/webmasters/answer/34592
- Analytics Help: Set up GA4 for a website  
  https://support.google.com/analytics/answer/9306384
