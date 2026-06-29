# Anata H1 Fix Report

Date: 2026-03-25  
Environment: production  
Method: WordPress REST API + Elementor template/page meta updates

## What Was Fixed

Two shared popup templates were outputting duplicate `H1` headings across core commercial pages:

- `Contact Us`
- `FREE ANALYSIS`

These were downgraded from `H1` to `H2` in the shared Elementor popup templates:

- `elementor_library/2187` `contact-form-popup`
- `elementor_library/2164` `pop-up`

Then the real hero headings were promoted to `H1` on these production pages:

- homepage `/`
- `/services/`
- `/services/shipping/`
- `/services/ai/`

## Backups

Backups were saved in:

- [2026-03-25-h1-fix](/Users/davidnarayan/Documents/Playground/runtime/pycache/Users/davidnarayan/Documents/anata_internal/website-ops/backups/2026-03-25-h1-fix)

Included backups:

- affected page JSON
- shared Elementor template JSON

## Verified Final State

Confirmed live `H1` output:

- `/` -> `Ecommerce Accelerator Partner.`
- `/services/` -> `Our Ecommerce Services.`
- `/services/shipping/` -> `Faster, Smarter, Stress-free Shipping.`
- `/services/ai/` -> `Faster, Smarter, Intelligent, Data.`
- `/services/fulfillment/` -> `Ecomm Fulfillment.`

Confirmed popup/form headings now render as `H2`:

- `Contact Us`
- `FREE ANALYSIS`

## Why This Matters

This removed duplicate/non-primary `H1` noise from the homepage and key service pages, which improves:

- primary topic clarity
- search intent signaling
- AI extraction quality
- semantic heading structure

## Next Recommended Fixes

1. Rewrite the homepage `H1` to a clearer commercial phrase.
2. Rewrite the AI page `H1`, which is still too vague.
3. Improve the fulfillment page `H1`, which is still weak and abbreviated.
4. Rework `/services/` into a tighter service hub.
5. Start building the Amazon cluster pages defined in the strategy docs.
