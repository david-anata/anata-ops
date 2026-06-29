import unittest
from unittest import mock

import hubspot_sales_os


class HubSpotSalesOsTests(unittest.TestCase):
    def test_infer_offer_prefers_fulfillment_when_progress_field_exists(self):
        deal = {
            "id": "1",
            "properties": {
                "dealname": "Acme Fulfillment Expansion",
                "service_type": "",
                "agency": "",
                "fulfillment": "active",
                "shipping_os": "",
            },
        }
        company = {"id": "22", "properties": {"name": "Acme", "service_type": "Fulfillment"}}

        inference = hubspot_sales_os.infer_offer(deal, company)

        self.assertEqual(inference["primary_offer"], "fulfillment")
        self.assertEqual(inference["deal_service_type_value"], "Fulfillment")
        self.assertGreaterEqual(inference["confidence"], 0.85)

    def test_run_writeback_preview_returns_high_confidence_actions(self):
        pipeline = {
            "id": "pipeline-1",
            "label": "Shared Sales",
            "stages": [
                {
                    "id": "qualified",
                    "label": "Qualified",
                    "metadata": {"probability": "0.2"},
                }
            ],
        }
        deal = {
            "id": "deal-1",
            "properties": {
                "dealname": "Amazon Ads Pilot",
                "dealstage": "qualified",
                "hubspot_owner_id": "owner-1",
                "service_type": "",
                "agency": "in_progress",
                "fulfillment": "",
                "shipping_os": "",
                "hs_next_step": "",
            },
        }
        company_associations = [
            {
                "from": {"id": "deal-1"},
                "to": [{"toObjectId": 3001, "associationTypes": [{"label": "Primary"}]}],
            }
        ]
        companies = [{"id": "3001", "properties": {"name": "Acme", "service_type": "Amazon"}}]

        with mock.patch.object(hubspot_sales_os, "get_primary_pipeline", return_value=pipeline), mock.patch.object(
            hubspot_sales_os, "search_recent_deals", return_value=[deal]
        ), mock.patch.object(
            hubspot_sales_os, "batch_read_associations", return_value=company_associations
        ), mock.patch.object(
            hubspot_sales_os, "batch_read_companies", return_value=companies
        ):
            result = hubspot_sales_os.run_writeback(mode="preview", limit=10)

        self.assertEqual(result["mode"], "preview")
        self.assertEqual(result["summary"]["candidateDeals"], 1)
        action_types = [action["type"] for action in result["deals"][0]["actions"]]
        self.assertIn("update_deal_service_type", action_types)
        self.assertIn("update_next_step", action_types)
