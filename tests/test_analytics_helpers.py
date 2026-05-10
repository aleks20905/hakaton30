import unittest
from unittest.mock import patch
import pandas as pd
import numpy as np
from analytics_helpers import (
    get_month_range, apply_family_filter, monthly_rollup, 
    current_month_kpis, family_breakdown, top_items
)

class TestAnalyticsHelpers(unittest.TestCase):

    def setUp(self):
        self.months = ["2025-01", "2025-02"]
        self.data = {
            "capacity": pd.DataFrame({"month_period": self.months, "available_gross_hours": [100.0, 200.0]}),
            "actuals": pd.DataFrame({
                "month_period": ["2025-01", "2025-01", "2025-02"], 
                "hours_consumed": [50.0, 20.0, 30.0],
                "quantity": [5, 2, 3],
                "family_code": ["A", "B", "A"],
                "item": ["I1", "I2", "I1"],
                "description": ["D1", "D2", "D1"],
                "transaction_date": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-02-01"])
            }),
            "backlog": pd.DataFrame({
                "month_period": ["2025-02"], 
                "total_labor_hours": [150.0],
                "family_code": ["A"]
            }),
            "forecast": pd.DataFrame({
                "month_period": ["2025-01", "2025-02"], 
                "required_labor_hours": [30.0, 40.0]
            })
        }

    def test_get_month_range(self):
        self.assertEqual(get_month_range("2025-01", "2025-03"), ["2025-01", "2025-02", "2025-03"])
        self.assertEqual(get_month_range("", "2025-01"), [])

    def test_apply_family_filter(self):
        # Already tests 'A' filter
        filtered = apply_family_filter(self.data, "B")
        self.assertEqual(len(filtered['actuals']), 1)
        self.assertTrue(all(filtered['actuals']['family_code'] == 'B'))

    def test_monthly_rollup(self):
        rollup = monthly_rollup(self.data, "2025-01", "2025-02")
        self.assertEqual(len(rollup), 2)
        # Jan: Act 70, Cap 100 -> Eff 70%
        self.assertEqual(rollup.iloc[0]['actual_hours'], 70.0)
        self.assertEqual(rollup.iloc[0]['operational_efficiency'], 70.0)

    def test_current_month_kpis(self):
        # We need to mock pd.Timestamp.today() to return a known month, 
        # or just test with a month that exists in the data
        with patch('pandas.Timestamp.today', return_value=pd.Timestamp("2025-01-15")):
            kpis = current_month_kpis(self.data)
            self.assertEqual(kpis['month'], "2025-01")
            self.assertEqual(kpis['actual_hours'], 70.0)
            self.assertEqual(kpis['capacity_hours'], 100.0)

    def test_family_breakdown(self):
        breakdown = family_breakdown(self.data, "2025-01", "2025-02")
        self.assertEqual(len(breakdown), 2)
        # Family A: 50+30 = 80 hours
        fam_a = next(item for item in breakdown if item['family_code'] == 'A')
        self.assertEqual(fam_a['hours'], 80.0)

    def test_top_items(self):
        top = top_items(self.data, "2025-01", "2025-02", n=1)
        self.assertEqual(len(top), 1)
        self.assertEqual(top[0]['item'], 'I1')

if __name__ == '__main__':
    unittest.main()
