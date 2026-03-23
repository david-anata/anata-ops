import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import ap_audit


ROOT = Path(__file__).resolve().parents[1]


class ApAuditTests(unittest.TestCase):
    def setUp(self):
        self.rules = ap_audit.load_rules(str(ROOT / "samples" / "rules.json"))
        self.transactions = ap_audit.normalize_transactions(
            ap_audit.load_rows(str(ROOT / "samples" / "transactions.csv")),
            self.rules,
        )
        self.tasks = ap_audit.normalize_tasks(
            ap_audit.load_rows(str(ROOT / "samples" / "clickup_tasks.csv")),
            self.rules,
        )
        self.as_of_date = date(2026, 3, 23)

    def test_grouped_vendor_rolls_into_existing_task(self):
        result = ap_audit.find_matches(self.transactions, self.tasks, self.rules, self.as_of_date)
        grouped_vendors = {item["vendor_name"] for item in result["grouped_rollups"]}
        self.assertIn("Google Workspace", grouped_vendors)
        self.assertIn("Adobe Creative Cloud", grouped_vendors)

    def test_missing_create_new_for_unmatched_vendor(self):
        result = ap_audit.find_matches(self.transactions, self.tasks, self.rules, self.as_of_date)
        create_vendors = {item["vendor_name"] for item in result["create_tasks"]}
        self.assertIn("Snowflake", create_vendors)

    def test_ambiguous_amzn_charge_becomes_exception(self):
        result = ap_audit.find_matches(self.transactions, self.tasks, self.rules, self.as_of_date)
        exception_vendors = {item["vendor"] for item in result["exceptions"]}
        self.assertIn("AMZN", exception_vendors)

    def test_default_data_dir_discovers_expected_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            (tmp / "transactions_2026_03_23.csv").write_text("reference,date,vendor,amount\nx,2026-03-23,TEST,1.00\n")
            (tmp / "clickup_tasks_latest.csv").write_text("task_name,vendor_name,amount_due\nTask,TEST,1.00\n")
            (tmp / "rules.json").write_text(json.dumps({"vendor_aliases": {"test": "Test"}}))
            args = ap_audit.parse_args.__globals__["argparse"].Namespace(
                transactions=None,
                clickup=None,
                clickup_token=None,
                clickup_list_id=None,
                clickup_view_id=None,
                rules=None,
                data_dir=str(tmp),
                mode="weekly",
                as_of_date="2026-03-23",
                lookback_days=7,
                payload_out=None,
                report_out=None,
                schema_report_out=None,
                slack_payload_out=None,
                apply_clickup_updates=False,
                post_slack=False,
            )
            transactions_path, clickup_path, rules_path = ap_audit.resolve_input_paths(args)
            self.assertTrue(transactions_path.endswith("transactions_2026_03_23.csv"))
            self.assertTrue(clickup_path.endswith("clickup_tasks_latest.csv"))
            self.assertTrue(rules_path.endswith("rules.json"))

    def test_clickup_task_to_row_maps_custom_fields(self):
        task = {
            "id": "abc123",
            "name": "Software | QuickBooks | Mar 2026",
            "due_date": "1773792000000",
            "status": {"status": "Upcoming"},
            "text_content": "AP note",
            "custom_fields": [
                {"name": "Vendor Name", "value": "QuickBooks", "type": "short_text"},
                {"name": "Amount Due", "value": "97.50", "type": "currency"},
                {"name": "Amount Paid", "value": "0", "type": "currency"},
                {"name": "Remaining Balance", "value": "97.50", "type": "currency"},
                {"name": "Billing Frequency", "value": "Monthly", "type": "short_text"},
                {"name": "Payment Method", "value": "Amex", "type": "short_text"},
                {"name": "Grouped Flag", "value": "false", "type": "checkbox"},
            ],
        }
        row = ap_audit.clickup_task_to_row(task)
        self.assertEqual(row["task_id"], "abc123")
        self.assertEqual(row["vendor_name"], "QuickBooks")
        self.assertEqual(row["amount_due"], "97.50")
        self.assertEqual(row["status"], "Upcoming")

    def test_schema_inspection_reports_missing_fields(self):
        manifest = ap_audit.load_schema_manifest()
        schema = ap_audit.inspect_clickup_schema(
            [
                {"id": "1", "name": "Amount Paid", "type": "currency"},
                {"id": "2", "name": "Remaining Balance", "type": "currency"},
            ],
            manifest,
        )
        self.assertGreater(schema["missing_field_count"], 0)
        self.assertEqual(schema["present_field_count"], 2)

    def test_build_payload_includes_extended_sections(self):
        weekly_summary = {"as_of_date": "2026-03-23"}
        leadership_summary = {"as_of_date": "2026-03-23", "critical_items": [], "material_new_charges": []}
        payload = ap_audit.build_payload(
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            weekly_summary,
            leadership_summary,
            [],
            {},
            {"actions": [], "skipped": []},
            {"mode": "weekly", "text": "summary"},
        )
        self.assertIn("weekly_summary", payload)
        self.assertIn("leadership_summary", payload)
        self.assertIn("bookkeeper_action_queue", payload)
        self.assertIn("clickup_update_actions", payload)
        self.assertIn("slack_payload", payload)
        self.assertIn("new_charge_alerts", payload)

    def test_daily_slack_payload_groups_sections(self):
        weekly_summary = {"as_of_date": "2026-03-23"}
        leadership_summary = {"critical_items": [], "material_new_charges": []}
        warnings = [
            {
                "vendor": "Test Vendor",
                "amount_due": 100.0,
                "remaining_balance": 100.0,
                "due_date": "2026-03-24",
                "message": "[HIGH] Test Vendor - $100.00 due on 2026-03-24.",
                "action": "Pay",
                "ap_state": "Due This Week",
                "level": "HIGH",
            }
        ]
        payload = ap_audit.build_slack_payload(
            mode="daily",
            weekly_summary=weekly_summary,
            leadership_summary=leadership_summary,
            warnings=warnings,
        )
        self.assertEqual(payload["mode"], "daily")
        self.assertTrue(payload["sections"])

    def test_slim_daily_slack_warnings_excludes_non_urgent_items(self):
        warnings = [
            {
                "vendor": "Due Soon",
                "amount_due": 100.0,
                "remaining_balance": 100.0,
                "due_date": "2026-03-24",
                "action": "Pay",
                "level": "HIGH",
                "ap_state": "Due This Week",
                "status": "Due This Week",
            },
            {
                "vendor": "Not Urgent",
                "amount_due": 50.0,
                "remaining_balance": 50.0,
                "due_date": "2026-03-30",
                "action": "Pay",
                "level": "MEDIUM",
                "ap_state": "Upcoming",
                "status": "Upcoming",
            },
        ]
        slim = ap_audit.slim_daily_slack_warnings(warnings, as_of_date=date(2026, 3, 23))
        self.assertEqual([item["vendor"] for item in slim], ["Due Soon"])

    def test_build_new_charge_alerts_includes_creates_and_exceptions(self):
        alerts = ap_audit.build_new_charge_alerts(
            transactions=self.transactions,
            tasks=self.tasks,
            creates=[ap_audit.build_create_task(self.transactions[0], self.rules, self.as_of_date)],
            exceptions=[
                {
                    "vendor": "AMZN",
                    "amount": 122.87,
                    "date": "2026-03-22",
                    "possible_matches": ["Ops task"],
                    "why_unclear": "descriptor is ambiguous",
                    "recommended_human_review_step": "check memo",
                    "confidence": 0.6,
                }
            ],
            material_amount=500.0,
        )
        self.assertEqual(len(alerts), 2)
        self.assertIn("UNKNOWN_REQUIRES_REVIEW", {item["alert_type"] for item in alerts})

    def test_extended_description_is_used_for_vendor_resolution(self):
        rows = [
            {
                "Posting Date": "3/19/2026",
                "Amount": "-843.14",
                "Description": "Transfer to Cap",
                "Extended Description": "Withdrawal ACH A TYPE: Stripe Cap CO: Anata    Entry Class Code: CCD    ACH Trace Number: 7",
                "Reference Number": "3939486718",
                "Type": "Retail ACH",
            }
        ]
        transactions = ap_audit.normalize_transactions(rows, self.rules)
        self.assertEqual(transactions[0].vendor_name, "Stripe Capital")

    def test_internal_a2a_transfer_is_excluded(self):
        rows = [
            {
                "Posting Date": "3/3/2026",
                "Amount": "-3400.00",
                "Description": "Withdrawal Home  A2A Transfer: ****5196",
                "Extended Description": "Withdrawal Home  A2A Transfer: ****5196",
                "Reference Number": "3910125776",
                "Type": "Withdrawal",
            }
        ]
        transactions = ap_audit.normalize_transactions(rows, self.rules)
        self.assertEqual(transactions, [])

    def test_build_clickup_update_actions_merges_group_rollups_per_task(self):
        schema_fields = [
            {"id": "amount-field", "name": "Amount*", "type": "currency", "type_config": {}},
            {"id": "grouped-flag", "name": "Grouped Flag", "type": "checkbox", "type_config": {}},
            {"id": "group-name", "name": "Group Name", "type": "short_text", "type_config": {}},
            {"id": "ap-state", "name": "AP State", "type": "drop_down", "type_config": {"options": [{"id": "grouped-id", "name": "Grouped"}]}},
            {"id": "reviewed", "name": "Last Reviewed Date", "type": "date", "type_config": {}},
            {"id": "audit", "name": "Last Audit Result", "type": "drop_down", "type_config": {"options": [{"id": "group-rollup", "name": "POSSIBLE_GROUPED_ITEM"}]}},
            {"id": "human", "name": "Needs Human Review", "type": "checkbox", "type_config": {}}
        ]
        actions = ap_audit.build_clickup_update_actions(
            self.tasks,
            [],
            [
                {"group_task_id": "task-001", "group_task_name": "Software | SaaS Rollup | Mar 2026", "vendor_name": "Google Workspace", "amount": 84.0, "sub_detail_note": "note-1"},
                {"group_task_id": "task-001", "group_task_name": "Software | SaaS Rollup | Mar 2026", "vendor_name": "Adobe Creative Cloud", "amount": 59.99, "sub_detail_note": "note-2"},
            ],
            schema_fields,
            self.as_of_date,
        )
        grouped_actions = [item for item in actions["actions"] if item["task_id"] == "task-001"]
        self.assertEqual(len(grouped_actions), 1)
        self.assertEqual(grouped_actions[0]["append_notes"], ["note-1", "note-2"])


if __name__ == "__main__":
    unittest.main()
