import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import io
import sys
import os

# Add the project root to sys.path to import script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from script import process_total_hours, process_backlog, process_routing, process_forecast, process_t_jit

class TestScriptProcessing(unittest.TestCase):

    def test_process_total_hours(self):
        # Create a dummy dataframe
        data = {
            'Month': ['September 2025', 'October 2025', 'Footer (hours)'],
            'Working Days': [20, 22, 0],
            'Gross Hrs': ['160', '176', '0']
        }
        df = pd.DataFrame(data)
        
        with patch('pandas.read_excel', return_value=df):
            processed_df = process_total_hours('dummy.xlsx')
            
            self.assertEqual(len(processed_df), 2)
            self.assertEqual(list(processed_df.columns), ['month', 'working_days', 'available_gross_hours'])
            self.assertEqual(processed_df['working_days'].tolist(), [20, 22])

    def test_process_backlog(self):
        data = {
            'FamilyCode': ['A', 'B'],
            'OrderNumber': ['123', '456'],
            'DueDate': ['2025-09-01', '2025-10-01'],
            'Description': ['Desc1', 'Desc2'],
            'Item': ['Item1', 'Item2'],
            'OpenOrderQty': [10, 20]
        }
        df = pd.DataFrame(data)
        
        with patch('pandas.read_excel', return_value=df):
            processed_df = process_backlog('dummy.xlsx')
            
            self.assertEqual(len(processed_df), 2)
            self.assertEqual(list(processed_df.columns), ['family_code', 'order_number', 'due_date', 'description', 'item', 'open_order_qty'])

    def test_process_routing(self):
        data = {
            'Item': ['Item1', 'Item1', 'Item2'],
            'Hours': [1.0, 1.2, 2.0]
        }
        df = pd.DataFrame(data)
        
        with patch('pandas.read_excel', return_value=df):
            processed_df = process_routing('dummy.xlsx')
            
            self.assertEqual(len(processed_df), 2)
            self.assertEqual(processed_df[processed_df['item'] == 'Item1']['hours_per_unit'].iloc[0], 1.2)

    def test_process_forecast(self):
        # Create a dataframe that mimics the structure in script.py
        # Row 1: empty/averages (index 0)
        # Row 2: header (index 1)
        # Rows 3+: data
        data = [
            ['Summary', 'Summary', 'Summary', 'Summary'],
            ['Family', 'Item', 'Description', '2026-Jan'],
            ['Fam1', 'Item1', 'Desc1', 10],
            ['Fam2', 'Item2', 'Desc2', 20]
        ]
        df = pd.DataFrame(data)
        
        with patch('pandas.read_excel', return_value=df):
            # The function uses pd.read_excel(file_path, header=None)
            # The read_excel call is in the function itself
            processed_df = process_forecast('dummy.xlsx')
            
            self.assertEqual(len(processed_df), 2)
            self.assertIn('forecast_qty', processed_df.columns)
            self.assertEqual(processed_df[processed_df['item'] == 'Item1']['forecast_qty'].iloc[0], 10)

    def test_process_t_jit(self):
        data = {
            'trans_date': ['2025-09-01', '2025-09-02'],
            'Item': ['Item1', 'Item2'],
            'Desc': ['Good', 'Sub-assem'],
            'Qty': [5, 5]
        }
        df = pd.DataFrame(data)
        
        valid_items = ['Item1']
        family_lookup = {'Item1': 'Fam1', 'Item2': 'Sub-assem'}
        
        with patch('pandas.read_excel', return_value=df):
            processed_df = process_t_jit('dummy.xlsx', valid_items, family_lookup)
            
            # Filtering:
            # 1. Item1 exists in valid_items
            # 2. Family code for Item1 is not Sub-assem
            self.assertEqual(len(processed_df), 1)
            self.assertEqual(processed_df['item'].iloc[0], 'Item1')

    def test_create_unified_schema_and_calculate_metrics(self):
        # Create small dummy dataframes
        capacity = pd.DataFrame({'month': ['2025-09-01'], 'working_days': [20], 'available_gross_hours': [160]})
        backlog = pd.DataFrame({
            'family_code': ['A'], 'order_number': ['1'], 'due_date': ['2025-09-01'],
            'description': ['D'], 'item': ['I'], 'open_order_qty': [10]
        })
        routing = pd.DataFrame({'item': ['I'], 'hours_per_unit': [2.0]})
        forecast = pd.DataFrame({
            'family': ['A'], 'item': ['I'], 'description': ['D'],
            'year_month': ['2025-09-01'], 'forecast_qty': [5]
        })
        t_jit = pd.DataFrame({
            'transaction_date': ['2025-09-01'], 'item': ['I'], 'description': ['D'],
            'quantity': [2], 'family_code': ['A']
        })
        
        # Helper to convert to datetime to match expected format
        capacity['month'] = pd.to_datetime(capacity['month'])
        backlog['due_date'] = pd.to_datetime(backlog['due_date']).dt.date
        forecast['year_month'] = pd.to_datetime(forecast['year_month'])
        t_jit['transaction_date'] = pd.to_datetime(t_jit['transaction_date']).dt.date
        
        from script import create_unified_schema, calculate_metrics
        
        unified = create_unified_schema(capacity, backlog, routing, forecast, t_jit)
        metrics = calculate_metrics(unified)
        
        self.assertIn('Monthly_Efficiency', metrics)
        self.assertEqual(len(metrics['Monthly_Efficiency']), 1)
        # Check calculation:
        # Backlog hours: 10 * 2.0 = 20
        # Actual hours: 2 * 2.0 = 4
        # Total consumed: 20 + 4 = 24
        # Capacity: 160
        # Ratio: 24 / 160 = 0.15 = 15%
        self.assertEqual(metrics['Monthly_Efficiency']['operational_efficiency_pct'].iloc[0], 15.0)

if __name__ == '__main__':
    unittest.main()
