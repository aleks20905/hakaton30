import os
import sys
import importlib
import unittest
from unittest.mock import patch
import pandas as pd

# Must be set BEFORE importing app, since app.py raises at import time
os.environ.setdefault("SECRET_KEY", "test-secret-key")

from app import app
class TestApp(unittest.TestCase):
    def setUp(self):
        app.config['TESTING'] = True
        self.app = app.test_client()

    @patch('app.load_data')
    def test_index_route_waiting(self, mock_load):
        mock_load.return_value = None
        response = self.app.get('/')
        self.assertEqual(response.status_code, 503)

    @patch('app.load_data')
    def test_index_route_success(self, mock_load):
        mock_data = {
            "capacity": pd.DataFrame({"month_period": ["2025-01"], "available_gross_hours": [100.0]}),
            "actuals": pd.DataFrame({
                "month_period": ["2025-01"], 
                "hours_consumed": [50.0],
                "quantity": [5],
                "item": ["I"],
                "description": ["D"],
                "transaction_date": pd.to_datetime(["2025-01-01"])
            }),
            "backlog": pd.DataFrame({
                "month_period": ["2025-01"], 
                "total_labor_hours": [10.0],
                "family_code": ["A"],
                "order_number": ["1"]
            }),
            "forecast": pd.DataFrame({
                "month_period": ["2025-01"], 
                "required_labor_hours": [20.0],
                "family": ["A"]
            }),
            "items": pd.DataFrame()
        }
        mock_load.return_value = mock_data
        
        response = self.app.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Dashboard', response.data)

    @patch('app.load_data')
    def test_balancing_route_auto(self, mock_load):
        # Setup similar data to index
        mock_data = {
            "capacity": pd.DataFrame({"month_period": ["2025-01"], "available_gross_hours": [100.0]}),
            "actuals": pd.DataFrame({
                "month_period": ["2025-01"], "hours_consumed": [50.0],
                "transaction_date": pd.to_datetime(["2025-01-01"])
            }),
            "backlog": pd.DataFrame({
                "month_period": ["2025-01"], "total_labor_hours": [10.0],
                "family_code": ["A"], "order_number": ["1"], "due_month": ["2025-01"]
            }),
            "forecast": pd.DataFrame({
                "month_period": ["2025-01"], "required_labor_hours": [20.0],
                "family": ["A"]
            })
        }
        mock_load.return_value = mock_data
        
        # Test auto balancing mode
        response = self.app.get('/balancing?mode=auto&target=90')
        self.assertEqual(response.status_code, 200)

class TestSecretKey(unittest.TestCase):
    def test_missing_secret_key_raises(self):
        """app.py must refuse to import without SECRET_KEY."""
        saved = os.environ.pop("SECRET_KEY", None)
        # Force a fresh import
        sys.modules.pop("app", None)
        try:
            with self.assertRaises(RuntimeError) as ctx:
                importlib.import_module("app")
            self.assertIn("SECRET_KEY", str(ctx.exception))
        finally:
            # Restore so other tests keep working
            if saved is not None:
                os.environ["SECRET_KEY"] = saved
            else:
                os.environ["SECRET_KEY"] = "test-secret-key"
            sys.modules.pop("app", None)
            importlib.import_module("app")

if __name__ == '__main__':
    unittest.main()

