# GA4 Lead Event Checklist

## Goal

Give Website Ops one reliable conversion signal so it can rank pages by search opportunity and actual lead-generation performance.

Primary event to implement:

- `generate_lead`

Optional supporting events:

- `click_phone`
- `click_email`
- `schedule_call`

## What counts as a lead

For Anata, `generate_lead` should fire only when a real website lead form is successfully submitted.

Use it for:

- contact form submits
- quote request submits
- free analysis submits
- strategy call booking forms, if they are on-site and confirmed

Do not use it for:

- generic CTA button clicks
- page views
- scroll depth
- outbound link clicks

## Best implementation path

### Option A: Thank-you page

Use this if the form redirects to a confirmation URL after submit.

1. Identify the confirmation URL for each lead form.
2. Create a derived GA4 event from `page_view`.
3. Event name: `generate_lead`
4. Condition: page location contains the thank-you URL.
5. Mark `generate_lead` as a key event in GA4.

This is the cleanest implementation if the site already redirects after submit.

### Option B: Inline form submit

Use this if the form submits without a page redirect.

1. Fire a GA4 event only after successful submit.
2. Event name: `generate_lead`
3. Pass parameters where possible:
   - `form_name`
   - `page_location`
   - `service_type`
4. Mark `generate_lead` as a key event in GA4.

This is usually done through:

- Google Tag Manager
- Elementor form success hook
- custom JS callback on successful submission

## Required validation

1. Submit a test lead form.
2. Open GA4 Realtime.
3. Confirm `generate_lead` appears.
4. Confirm it is marked as a key event.
5. Confirm the correct landing page is associated with the conversion path.

## Minimum event map for Anata

- `/contact/` form submit -> `generate_lead`
- `/free-marketing-analysis/` form submit -> `generate_lead`
- service page quote or analysis forms -> `generate_lead`

If multiple forms exist, use one event name with parameters rather than multiple event names.

## What Website Ops needs after setup

Website Ops should assume:

- `generate_lead` is the primary conversion event
- GA4 page-level conversion comparisons should use that event
- pages with traffic and zero `generate_lead` conversions become conversion-fix candidates

## Definition of done

- `generate_lead` exists in GA4
- `generate_lead` is marked as a key event
- one live test submission has been validated
- Website Ops can compare sessions versus conversions by landing page with confidence

## References

- Google Analytics recommended events: https://support.google.com/analytics/answer/9268036
- Mark events as key events: https://support.google.com/analytics/answer/13128484
- Lead generation key-event setup: https://support.google.com/analytics/answer/12966437?hl=en-EN
