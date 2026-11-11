import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ProjectContractTests(unittest.TestCase):
    def test_four_unique_dashboards_are_provisioned(self):
        paths = sorted((ROOT / "grafana" / "dashboards").glob("*.json"))
        self.assertEqual(4, len(paths))
        dashboards = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
        self.assertEqual(4, len({item["uid"] for item in dashboards}))
        self.assertTrue(all(item["panels"] for item in dashboards))

    def test_business_dashboard_has_required_filters_and_panels(self):
        path = ROOT / "grafana" / "dashboards" / "fraud-business-analytics.json"
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        variables = {item["name"] for item in dashboard["templating"]["list"]}
        titles = {item["title"] for item in dashboard["panels"]}
        self.assertEqual({"state", "merch"}, variables)
        self.assertIn("Score density by selected state and merchant", titles)
        self.assertIn("Filtered processing TPS", titles)
        self.assertIn("Fraud share by category in last 1000 transactions", titles)

    def test_alert_rules_are_present(self):
        text = (ROOT / "grafana" / "provisioning" / "alerting" / "rules.yaml").read_text()
        self.assertEqual(2, text.count("      - uid:"))
        self.assertIn("fraud-rate-high", text)
        self.assertIn("transaction-stream-stopped", text)

    def test_raw_datasets_are_ignored(self):
        ignored = (ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertIn("*.csv", ignored)
        self.assertIn("data/", ignored)


if __name__ == "__main__":
    unittest.main()
