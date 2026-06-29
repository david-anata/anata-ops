## Sitemap Submission

Date: 2026-03-27

### Public Sitemap State
- `https://anatainc.com/sitemap_index.xml` returns `404`
- `https://anatainc.com/wp-sitemap.xml` returns `200` and serves a valid XML sitemap index

### Search Console Action
- Submitted `https://anatainc.com/wp-sitemap.xml` to the Search Console domain property `sc-domain:anatainc.com`
- Submission response: `204`
- Verified in sitemap list:
  - path: `https://anatainc.com/wp-sitemap.xml`
  - pending: `true`
  - warnings: `0`
  - errors: `0`

### Operational Impact
- Google now has an active sitemap submission that points to the live working WordPress core sitemap.
- This restores crawler discovery coverage even though the legacy `/sitemap_index.xml` path is still broken.

### Remaining Follow-Up
- Add a permanent redirect from `/sitemap_index.xml` to `/wp-sitemap.xml`
- Add `Sitemap: https://anatainc.com/wp-sitemap.xml` to `robots.txt` if possible
- Re-run Website Ops after crawler-facing sitemap handling is updated
