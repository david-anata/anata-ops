# Service Route Normalization Plan

## Goal

Consolidate Anata service authority under one canonical route family:

- keep: `/services/...`
- retire: `/ecommerce-services/...`

This removes authority split between old and current service paths.

## Live state confirmed

Canonical-family pages already living under `/services/`:

- `https://anatainc.com/services/`
- `https://anatainc.com/services/fulfillment/` (WP page `2640`)
- `https://anatainc.com/services/shipping/` (WP page `5540`)
- `https://anatainc.com/services/ai/` (WP page `6111`)

Legacy service pages still live under `/ecommerce-services/`:

- `https://anatainc.com/ecommerce-services/advertising/` (WP page `896`)
- `https://anatainc.com/ecommerce-services/web-design/` (WP page `318`)

Redirect behavior confirmed:

- `/services/advertising/` -> `301` -> `/ecommerce-services/advertising/`
- `/services/web-design/` -> `301` -> `/ecommerce-services/web-design/`
- `/web-design/` -> `301` -> `/ecommerce-services/web-design/`

## Required normalization decisions

### Advertising

Decision:

- create or restore canonical page: `/services/advertising/`

Then:

1. move or rebuild the current advertising page content onto `/services/advertising/`
2. set canonical to `/services/advertising/`
3. update internal links to `/services/advertising/`
4. `301` redirect `/ecommerce-services/advertising/` -> `/services/advertising/`

### Web Design

This needs a business decision first.

Option A: Web Design remains an active service

1. create canonical page: `/services/web-design/`
2. migrate or rebuild the current content there
3. set canonical to `/services/web-design/`
4. update internal links to `/services/web-design/`
5. `301` redirect:
   - `/ecommerce-services/web-design/` -> `/services/web-design/`
   - `/web-design/` -> `/services/web-design/`

Option B: Web Design is no longer a priority commercial service

1. choose the best-fit destination:
   - `/services/`
   - or another related active service page
2. `301` redirect:
   - `/ecommerce-services/web-design/` -> chosen destination
   - `/services/web-design/` -> chosen destination
   - `/web-design/` -> chosen destination
3. remove internal links pointing to legacy web-design URLs

## Internal-link standard

After normalization, all nav, CTA, footer, and in-body commercial links should point only to:

- `/services/...`

No internal links should continue pointing to `/ecommerce-services/...`

## Canonical standard

For every active service:

- the page should self-canonicalize
- the monitored URL, final URL, and canonical URL should all match

## Sitemap standard

Only canonical service URLs should appear in the XML sitemap.

Important note:

- `https://anatainc.com/sitemap_index.xml` is currently returning a 404 page instead of a sitemap response

That should be corrected after route normalization so search engines receive the intended sitemap files.

## Order of operations

1. confirm `/services/` is the permanent service architecture
2. decide whether `web-design` remains an active service
3. create missing canonical `/services/...` pages where needed
4. update page canonicals
5. update internal links
6. apply `301` redirects from legacy routes
7. regenerate or fix sitemap output
8. request reindexing in Search Console for the canonical service pages

## Definition of done

- all active services live under `/services/...`
- all legacy `/ecommerce-services/...` service URLs redirect to the chosen canonical target
- all internal links use canonical service URLs only
- all canonical tags align to the final service URL
- the sitemap exposes only canonical service URLs
