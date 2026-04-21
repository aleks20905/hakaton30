import pandas as pd
import numpy as np
from pathlib import Path
import warnings
import re
warnings.filterwarnings('ignore')

INPUT_FILES = {
    'total_hours': 'Total_Hours.xlsx',
    'backlog': 'BACKLOG_Export.xlsx', 
    'routing': 'Routing_PL.xlsx',
    'forecast': 'Forecast.xlsx',
    't_jit': 'T_JIT.xlsx'
}

OUTPUT_FILE = 'Unified_Production_Analytics.xlsx'


def process_total_hours(file_path):
    """
    Keep only Month, Working days, Gross Hrs.
    Handles "September 2025" format and ignores footer table starting with "(hours)"
    """
    df = pd.read_excel(file_path)
    
    # Clean up column names (strip spaces)
    df.columns = [str(c).strip() for c in df.columns]
    
    # Find header row (skip any initial empty rows )
    # Keep only rows where Month column matches "Month Year" ("September 2025")
    # excludes the footer table starting with "(hours)"
    month_col = next((c for c in df.columns if 'month' in c.lower()), None)
    
    # Filter valid rows: must contain a month name and a year (2025 or 2026)
    def is_valid_month_row(val):
        if pd.isna(val):
            return False
        val_str = str(val).strip()
        return bool(re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+202[56]', val_str))
    
    df = df[df[month_col].apply(is_valid_month_row)].copy()
    
    # jkeep only the columns we need
    working_col = next((c for c in df.columns if 'working' in c.lower() and 'day' in c.lower()), None)
    gross_col = next((c for c in df.columns if 'gross' in c.lower() and 'hr' in c.lower()), None)
    
    df_clean = df[[month_col, working_col, gross_col]].copy()
    df_clean.columns = ['month', 'working_days', 'gross_hours']
    
    # Parse "September 2025" 
    df_clean['month'] = pd.to_datetime(df_clean['month'], format='%B %Y')
    
    # Clean numeric columns (remove commas, convert to float)
    for col in ['working_days', 'gross_hours']:
        df_clean[col] = pd.to_numeric(df_clean[col].astype(str).str.replace(',', '').str.strip(), errors='coerce')
    
    df_clean = df_clean.rename(columns={'gross_hours': 'available_gross_hours'})
    
    return df_clean

# ==============================================================================
# PROCESS BACKLOG 
# ==============================================================================
def process_backlog(file_path):
    """Keep: FamilyCode, OrderNumber, DueDate, Description, Item, OpenOrderQty (removed Line and order_line_key)"""
    df = pd.read_excel(file_path)
    
    def find_col(cols, keywords):
        return next((c for c in cols if all(kw.lower() in c.lower() for kw in keywords)), None)
    
    cols_to_keep = {
        'family_code': find_col(df.columns, ['family', 'code']),
        'order_number': find_col(df.columns, ['order', 'number']),
        'due_date': find_col(df.columns, ['due', 'date']),
        'description': find_col(df.columns, ['description']),
        'item': find_col(df.columns, ['item']),
        'open_order_qty': find_col(df.columns, ['open', 'qty']) or find_col(df.columns, ['openorder', 'qty'])
    }
    
    df_clean = df[list(cols_to_keep.values())].copy()
    df_clean.columns = list(cols_to_keep.keys())
    
    df_clean['due_date'] = pd.to_datetime(df_clean['due_date']).dt.date
    
    # Preserve FamilyCode exactly as string for "Sub-assem" matching
    df_clean['family_code'] = df_clean['family_code'].astype(str).str.strip()
    
    df_clean['item'] = df_clean['item'].astype(str).str.strip()
    
    return df_clean

# ==============================================================================
# PROCESS ROUTING 
# ==============================================================================
def process_routing(file_path):
    """Keep only Item, Hours (cumulative), deduplicate by max hours"""
    df = pd.read_excel(file_path)
    
    item_col = next((c for c in df.columns if c.lower().strip() == 'item'), None)
    hours_col = next((c for c in df.columns if 'hours' in c.lower() or 'labor' in c.lower()), None)
    
    df_clean = df[[item_col, hours_col]].copy()
    df_clean.columns = ['item', 'hours_per_unit']
    
    df_clean['item'] = df_clean['item'].astype(str).str.strip()
    
    # Round 
    df_clean['hours_per_unit'] = pd.to_numeric(df_clean['hours_per_unit'], errors='coerce').round(3)
    
    # Deduplicate
    df_clean = df_clean.groupby('item')['hours_per_unit'].max().reset_index()
    
    return df_clean

# ==============================================================================
# PROCESS FORECAST 
# ==============================================================================
def process_forecast(file_path):
    """
    Skip first row (averages), use second row as header.
    Handles month columns in format: 2026-Jan or January 2026
    Returns: family, item, description, year_month (as datetime), forecast_qty
    """
    # Read without header first to skip row 1 (index 0) and use row 2 (index 1) as header
    df_raw = pd.read_excel(file_path, header=None)
    
    # Skip first row (summary/averages), use second row as column names
    df_data = df_raw.iloc[1:].copy()
    df_data.columns = df_raw.iloc[1]  # Row 2 becomes header
    
    # Drop any completely empty rows/columns
    df_data = df_data.dropna(how='all').dropna(axis=1, how='all')
    
    # Identify identifier columns vs month columns
    id_cols = []
    month_cols = []
    
    for c in df_data.columns:
        col_str = str(c).strip()
        if any(x in col_str for x in ['Family', 'Item', 'Description']) and '202' not in col_str:
            id_cols.append(c)
        elif '2026' in col_str or '2027' in col_str or '2028' in col_str:
            month_cols.append(c)
    
    # Hard select only these columns
    df_clean = df_data[id_cols + month_cols].copy()
    
    # Rename columns
    rename_map = {
        next((c for c in id_cols if 'family' in str(c).lower()), None): 'family',
        next((c for c in id_cols if 'item' in str(c).lower()), None): 'item',
        next((c for c in id_cols if 'desc' in str(c).lower()), None): 'description'
    }
    rename_map = {k: v for k, v in rename_map.items() if k is not None}
    df_clean = df_clean.rename(columns=rename_map)
    
    # Clean description (remove << prefixes)
    df_clean['description'] = df_clean['description'].astype(str).str.replace(r'^<<\s*', '', regex=True)
    
    df_clean['item'] = df_clean['item'].astype(str).str.strip()
    
    # Reshape: Wide to Long 
    df_long = df_clean.melt(
        id_vars=['family', 'item', 'description'],
        value_vars=month_cols,
        var_name='year_month_str',
        value_name='forecast_qty'
    )
    
    # Parse year_month_str - handle both "2026-Jan" and "January 2026" formats
    def parse_month_year(val):
        val = str(val).strip()
        try:
            # Try "2026-Jan" format first
            return pd.to_datetime(val, format='%Y-%b')
        except:
            try:
                # Try "January 2026" format
                return pd.to_datetime(val, format='%B %Y')
            except:
                return pd.NaT
    
    df_long['year_month'] = df_long['year_month_str'].apply(parse_month_year)
    
    # Cast forecast_qty to integer, filling NaN with 0
    df_long['forecast_qty'] = pd.to_numeric(df_long['forecast_qty'], errors='coerce').fillna(0).astype(int)
    
    # Drop zero forecasts to save space 
    df_long = df_long[df_long['forecast_qty'] != 0].copy()
    
    return df_long[['family', 'item', 'description', 'year_month', 'forecast_qty']]

# ==============================================================================
# PROCESS T_JIT 
# ==============================================================================
def process_t_jit(file_path, valid_items, family_lookup):
    """
    Keep: trans_date, item, description, qty
    Filters:
    1. Description not starting with <<, PCB, PCA
    2. Item exists in valid_items (Forecast OR Backlog)
    3. Family code is not "Sub-assem"
    """
    df = pd.read_excel(file_path)
    
    # Find columns
    date_col = next((c for c in df.columns if 'trans_date' in c.lower()), None)
    item_col = next((c for c in df.columns if c.lower().strip() == 'item'), None)
    desc_col = next((c for c in df.columns if 'desc' in c.lower()), None)
    qty_col = next((c for c in df.columns if c.lower().strip() in ['qty', 'quantity']), None)
    
    df_clean = df[[date_col, item_col, desc_col, qty_col]].copy()
    df_clean.columns = ['transaction_date', 'item', 'description', 'quantity']
    
    df_clean['transaction_date'] = pd.to_datetime(df_clean['transaction_date']).dt.date
    
    df_clean['item'] = df_clean['item'].astype(str).str.strip()
    
    # FILTER 1: Description filters
    desc = df_clean['description'].astype(str)
    mask_desc = ~(
        desc.str.startswith('<<', na=False) |
        desc.str.upper().str.startswith('PCB', na=False) |
        desc.str.upper().str.startswith('PCA', na=False)
    )
    df_clean = df_clean[mask_desc].copy()
    
    # FILTER 2: Item must exist in Forecast OR Backlog master lists
    valid_items_set = set(valid_items)
    df_clean = df_clean[df_clean['item'].isin(valid_items_set)].copy()
    
    # FILTER 3: Family code is not "Sub-assem"
    df_clean['family_code'] = df_clean['item'].map(family_lookup)
    df_clean = df_clean[df_clean['family_code'] != 'Sub-assem'].copy()
    
    return df_clean

# ==============================================================================
# CREATE UNIFIED SCHEMAS, CALCULATE METRICS 
# ==============================================================================
def create_unified_schema(capacity, backlog, routing, forecast, t_jit):
    
    # --- Sheet 1: Capacity_Calendar ---
    capacity_out = capacity.copy()
    
    # --- Sheet 2: Item_Master ---
    item_family = backlog[['item', 'family_code']].drop_duplicates()

    forecast_family = forecast[['item', 'family']].rename(columns={'family': 'family_code'}).drop_duplicates()
    
    all_families = pd.concat([item_family, forecast_family]).drop_duplicates(subset=['item'], keep='first')
    
    item_master = routing.merge(all_families, on='item', how='left')
    item_master = item_master[['item', 'family_code', 'hours_per_unit']]
    
    # Add description from forecast if available
    desc_map = forecast[['item', 'description']].drop_duplicates(subset=['item'])
    item_master = item_master.merge(desc_map, on='item', how='left')
    
    # Reorder columns
    item_master = item_master[['item', 'family_code', 'hours_per_unit', 'description']]
    
    # --- Sheet 3: Demand_Backlog ---
    # Join with routing to get hours per unit
    demand_backlog = backlog.merge(routing[['item', 'hours_per_unit']], on='item', how='left')
    demand_backlog['total_labor_hours'] = demand_backlog['open_order_qty'] * demand_backlog['hours_per_unit']
    demand_backlog = demand_backlog.round({'hours_per_unit': 3, 'total_labor_hours': 2})
    
    demand_backlog = demand_backlog[['family_code', 'order_number', 'due_date', 'description', 
                                      'item', 'open_order_qty', 'hours_per_unit', 'total_labor_hours']]
    
    # --- Sheet 4: Production_Forecast
    forecast_enriched = forecast.merge(routing[['item', 'hours_per_unit']], on='item', how='left')
    forecast_enriched['required_labor_hours'] = forecast_enriched['forecast_qty'] * forecast_enriched['hours_per_unit']
    forecast_enriched = forecast_enriched.round({'hours_per_unit': 3, 'required_labor_hours': 2})
    
    # Keep only: family, item, description, year_month, forecast_qty, hours_per_unit, required_labor_hours
    forecast_enriched = forecast_enriched[['family', 'item', 'description', 'year_month', 
                                            'forecast_qty', 'hours_per_unit', 'required_labor_hours']]
    
    # --- Sheet 5: Production_Actuals ---
    actuals_enriched = t_jit.merge(routing[['item', 'hours_per_unit']], on='item', how='left')
    actuals_enriched['hours_consumed'] = actuals_enriched['quantity'] * actuals_enriched['hours_per_unit']
    actuals_enriched = actuals_enriched.round({'hours_per_unit': 3, 'hours_consumed': 2})
    
    actuals_enriched = actuals_enriched[['transaction_date', 'item', 'description', 'quantity', 
                                          'family_code', 'hours_per_unit', 'hours_consumed']]
    
    return {
        'Capacity_Calendar': capacity_out,
        'Item_Master': item_master,
        'Demand_Backlog': demand_backlog,
        'Production_Forecast': forecast_enriched,
        'Production_Actuals': actuals_enriched
    }

# ==============================================================================
# CALCULATE EFFICIENCY METRICS
# ==============================================================================
def calculate_metrics(dfs):
    """Calculate efficiency metrics: 
    1. (Actual + Backlog) / Total Hours  
    2. (Forecast * Routing) / Total Hours"""
    capacity = dfs['Capacity_Calendar'].copy()
    backlog = dfs['Demand_Backlog'].copy()
    forecast = dfs['Production_Forecast'].copy()
    actuals = dfs['Production_Actuals'].copy()
    
    metrics = {}
    
    # month_period is only for grouping 
    capacity['month_period'] = pd.to_datetime(capacity['month']).dt.to_period('M')
    backlog['month_period'] = pd.to_datetime(backlog['due_date']).dt.to_period('M')
    
    if not actuals.empty:
        actuals['month_period'] = pd.to_datetime(actuals['transaction_date']).dt.to_period('M')
    
    # Ensure forecast has month_period
    forecast['month_period'] = pd.to_datetime(forecast['year_month']).dt.to_period('M')
    
    # --- SUMMARIZE BY MONTH ---
    
    # 1. Sum Backlog hours by due_date month
    backlog_monthly = backlog.groupby('month_period')['total_labor_hours'].sum().reset_index()
    backlog_monthly.columns = ['month_period', 'backlog_labor_hours']
    
    # 2. Sum Actual hours by transaction_date month
    if not actuals.empty:
        actuals_monthly = actuals.groupby('month_period')['hours_consumed'].sum().reset_index()
        actuals_monthly.columns = ['month_period', 'actual_hours_consumed']
    else:
        actuals_monthly = pd.DataFrame(columns=['month_period', 'actual_hours_consumed'])
    
    # 3. Sum Forecast hours (Forecast Qty * Routing) by month
    forecast_monthly = forecast.groupby('month_period')['required_labor_hours'].sum().reset_index()
    forecast_monthly.columns = ['month_period', 'forecast_labor_hours']
    
    # --- MERGE WITH CAPACITY ---
    efficiency = capacity[['month_period', 'available_gross_hours']].merge(
        backlog_monthly, on='month_period', how='left'
    ).merge(
        actuals_monthly, on='month_period', how='left'
    ).merge(
        forecast_monthly, on='month_period', how='left'
    ).fillna(0)
    
    # --- CALCULATE EFFICIENCY METRICS ---
    
    # Metric 1: (Backlog + Actual) / Capacity (Operational Efficiency)
    efficiency['total_consumed_hours'] = efficiency['backlog_labor_hours'] + efficiency['actual_hours_consumed']
    efficiency['operational_efficiency_ratio'] = efficiency['total_consumed_hours'] / efficiency['available_gross_hours']
    efficiency['operational_efficiency_pct'] = (efficiency['operational_efficiency_ratio'] * 100).round(2)
    
    # Metric 2: (Forecast * Routing) / Capacity (Forecast Load/Coverage)
    efficiency['forecast_efficiency_ratio'] = efficiency['forecast_labor_hours'] / efficiency['available_gross_hours']
    efficiency['forecast_efficiency_pct'] = (efficiency['forecast_efficiency_ratio'] * 100).round(2)
    
    # Metric 3: Total Load (Backlog + Forecast) / Capacity
    efficiency['total_demand_hours'] = efficiency['backlog_labor_hours'] + efficiency['forecast_labor_hours']
    efficiency['total_load_ratio'] = efficiency['total_demand_hours'] / efficiency['available_gross_hours']
    efficiency['total_load_pct'] = (efficiency['total_load_ratio'] * 100).round(2)
    
    # Add readable month name
    efficiency['month_name'] = efficiency['month_period'].astype(str)
    
    # --- SHEET 1: Monthly Efficiency Summary ---
    metrics['Monthly_Efficiency'] = efficiency[[
        'month_name', 
        'available_gross_hours',
        'actual_hours_consumed',
        'backlog_labor_hours',
        'forecast_labor_hours',
        'total_consumed_hours',
        'operational_efficiency_pct',
        'forecast_efficiency_pct',
        'total_load_pct'
    ]].copy()
    
    # --- SHEET 2: Detailed Capacity Load Analysis ---
    metrics['Capacity_Load_Analysis'] = efficiency[[
        'month_name',
        'available_gross_hours',
        'actual_hours_consumed',      # T_JIT * Routing (what was done)
        'backlog_labor_hours',        # Backlog * Routing (what needs to be done)
        'forecast_labor_hours',       # Forecast * Routing (what is planned)
        'total_consumed_hours',       # Actual + Backlog
        'total_demand_hours',         # Backlog + Forecast
        'operational_efficiency_pct', # (Actual+Backlog)/Capacity %
        'forecast_efficiency_pct',    # (Forecast)/Capacity %  <-- NEW!
        'total_load_pct'              # (Backlog+Forecast)/Capacity %
    ]].copy()
    
    # --- SHEET 3: Forecast vs Capacity (Dedicated view) ---
    forecast_analysis = efficiency[[
        'month_name',
        'available_gross_hours',
        'forecast_labor_hours',
        'forecast_efficiency_ratio',
        'forecast_efficiency_pct'
    ]].copy()
    forecast_analysis['capacity_remaining'] = forecast_analysis['available_gross_hours'] - forecast_analysis['forecast_labor_hours']
    forecast_analysis['capacity_remaining_pct'] = ((1 - forecast_analysis['forecast_efficiency_ratio']) * 100).round(2)
    
    metrics['Forecast_Efficiency'] = forecast_analysis
    
    return metrics

def main():
    print("Starting Production Data Integration...")
    
    try:
        print("1. Processing Total_Hours...")
        capacity = process_total_hours(INPUT_FILES['total_hours'])
        print(f"   ✓ Loaded {len(capacity)} capacity periods")
        
        print("2. Processing Backlog...")
        backlog = process_backlog(INPUT_FILES['backlog'])
        print(f"   ✓ Loaded {len(backlog)} backlog lines")
        
        print("3. Processing Routing...")
        routing = process_routing(INPUT_FILES['routing'])
        print(f"   ✓ Loaded {len(routing)} routing records")
        
        print("4. Processing Forecast (reshaping wide to long)...")
        forecast = process_forecast(INPUT_FILES['forecast'])
        print(f"   ✓ Created {len(forecast)} forecast records (long format)")
        
        print("5. Processing T_JIT (applying filters)...")
        valid_items = list(set(forecast['item'].unique()) | set(backlog['item'].unique()))
        family_lookup = pd.concat([
            backlog[['item', 'family_code']],
            forecast[['item', 'family']].rename(columns={'family': 'family_code'})
        ]).set_index('item')['family_code'].to_dict()
        
        t_jit = process_t_jit(INPUT_FILES['t_jit'], valid_items, family_lookup)
        print(f"   ✓ Loaded {len(t_jit)} JIT transactions after filtering")
        
        print("6. Creating unified schema...")
        unified = create_unified_schema(capacity, backlog, routing, forecast, t_jit)
        
        print("7. Calculating efficiency metrics...")
        metrics = calculate_metrics(unified)
        
        print(f"8. Writing output to {OUTPUT_FILE}...")
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            for sheet_name, df in unified.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"   - {sheet_name}: {len(df)} rows")
            
            for sheet_name, df in metrics.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"   - {sheet_name}: {len(df)} rows")
        
        print("\n✅ Processing complete!")
        print(f"\nSummary:")
        print(f"  - Capacity periods: {len(unified['Capacity_Calendar'])}")
        print(f"  - Unique items in master: {len(unified['Item_Master'])}")
        print(f"  - Backlog orders: {len(unified['Demand_Backlog'])}")
        print(f"  - Forecast records (long): {len(unified['Production_Forecast'])}")
        print(f"  - T_JIT transactions (filtered): {len(unified['Production_Actuals'])}")
        
        months_sample = ', '.join(capacity['month'].dt.strftime('%B %Y').head(3).tolist())
        print(f"  - Sample months detected: {months_sample}...")
        
    except Exception as e:
        print(f"\n Error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
