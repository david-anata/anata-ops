import json
import unittest
from unittest import mock

import hubspot_sales


class HubSpotSalesTests(unittest.TestCase):
    def test_create_deal_posts_json_payload_and_parses_response(self):
        captured = {}
        response_payload = {
            "id": "deal-123",
            "createdAt": "2026-06-29T12:00:00Z",
            "updatedAt": "2026-06-29T12:00:00Z",
            "archived": False,
            "properties": {"dealname": "Acme Fulfillment Opportunity"},
        }

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(response_payload).encode("utf-8")

        def fake_urlopen(request, timeout=0):
            captured["request"] = request
            captured["timeout"] = timeout
            return FakeResponse()

        deal_request = hubspot_sales.DealCreateRequest(
            properties={
                "dealname": "Acme Fulfillment Opportunity",
                "pipeline": "default",
                "dealstage": "appointmentscheduled",
                "anata_service_line": "fulfillment",
                "anata_lead_source_detail": "website",
                "hubspot_owner_id": "12345",
            },
            company_id="111",
            contact_id="222",
        )
        with mock.patch.dict("os.environ", {"HUBSPOT_PORTAL_ID": "999"}, clear=False):
            with mock.patch("hubspot_sales.urllib.request.urlopen", side_effect=fake_urlopen):
                deal = hubspot_sales.create_deal(deal_request, token="private-token")

        request = captured["request"]
        self.assertEqual(request.full_url, "https://api.hubapi.com/crm/v3/objects/deals")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Authorization"), "Bearer private-token")
        self.assertEqual(request.get_header("Content-type"), "application/json")
        payload = json.loads(request.data.decode("utf-8"))
        self.assertEqual(payload["properties"]["dealname"], "Acme Fulfillment Opportunity")
        self.assertEqual(payload["associations"][0]["to"]["id"], "222")
        self.assertEqual(payload["associations"][1]["to"]["id"], "111")
        self.assertEqual(deal["id"], "deal-123")
        self.assertEqual(deal["hubspot_url"], "https://app.hubspot.com/contacts/999/record/0-3/deal-123")

    def test_validate_deal_create_request_uses_rules_config(self):
        rules = hubspot_sales.read_sales_rules()
        request = hubspot_sales.DealCreateRequest(
            properties={
                "dealname": "Acme",
                "pipeline": "default",
                "anata_service_line": "fulfillment",
                "anata_lead_source_detail": "website",
                "hubspot_owner_id": "123",
            },
            company_id="111",
            contact_id="222",
        )
        errors = hubspot_sales.validate_deal_create_request(request, rules)
        self.assertIn("Missing required deal property: dealstage", errors)


if __name__ == "__main__":
    unittest.main()
