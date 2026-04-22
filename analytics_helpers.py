"""
Pure data-processing helpers. No Flask, no I/O.
All functions take a `data` dict: {capacity, backlog, forecast, actuals, items}
"""
import pandas as pd
import numpy as np


def get_month_range(start: str, end: str) -> list:
    """Return list of YYYY-MM strings from start to end inclusive."""
    if not start or not end:
        return []
    idx = pd.period_range(start=start, end=end, freq="M")
    return [p.strftime("%Y-%m") for p in idx]


def apply_family_filter(data: dict, family: str) -> dict:
    """Return a new data dict with family_code filter applied (if not 'all')."""
    if not family or family == "all":
        return data
    out = {}
    for key, df in data.items():
        if "family_code" in df.columns:
            out[key] = df[df["family_code"].astype(str) == family].copy()
        else:
            out[key] = df
    return out


def monthly_rollup(data: dict, start: str, end: str) -> pd.DataFrame:
    """
    Build the core monthly table:
    month_period | capacity_hours | actual_hours | backlog_hours | forecast_hours
                 | operational_efficiency | load_ratio
    """
    months = get_month_range(start, end)
    base = pd.DataFrame({"month_period": months})

    # Capacity
    cap = data["capacity"]
    if not cap.empty and "month_period" in cap.columns:
        cap_agg = cap.groupby("month_period", as_index=False)["available_gross_hours"].sum()
        cap_agg = cap_agg.rename(columns={"available_gross_hours": "capacity_hours"})
        base = base.merge(cap_agg, on="month_period", how="left")
    else:
        base["capacity_hours"] = 0.0

    # Actuals
    act = data["actuals"]
    if not act.empty and "hours_consumed" in act.columns:
        act_agg = act.groupby("month_period", as_index=False)["hours_consumed"].sum()
        act_agg = act_agg.rename(columns={"hours_consumed": "actual_hours"})
        base = base.merge(act_agg, on="month_period", how="left")
    else:
        base["actual_hours"] = 0.0

    # Backlog
    bl = data["backlog"]
    if not bl.empty and "total_labor_hours" in bl.columns:
        bl_agg = bl.groupby("month_period", as_index=False)["total_labor_hours"].sum()
        bl_agg = bl_agg.rename(columns={"total_labor_hours": "backlog_hours"})
        base = base.merge(bl_agg, on="month_period", how="left")
    else:
        base["backlog_hours"] = 0.0

    # Forecast
    fc = data["forecast"]
    if not fc.empty and "required_labor_hours" in fc.columns:
        fc_agg = fc.groupby("month_period", as_index=False)["required_labor_hours"].sum()
        fc_agg = fc_agg.rename(columns={"required_labor_hours": "forecast_hours"})
        base = base.merge(fc_agg, on="month_period", how="left")
    else:
        base["forecast_hours"] = 0.0

    # Fill NaN with 0
    for col in ["capacity_hours", "actual_hours", "backlog_hours", "forecast_hours"]:
        base[col] = base[col].fillna(0).round(1)

    # Efficiency metrics (safe divide)
    base["operational_efficiency"] = np.where(
        base["capacity_hours"] > 0,
        (( base["backlog_hours"] + base["actual_hours"]) / base["capacity_hours"] * 100).round(1),
        0,
    )
    base["load_ratio"] = np.where(
        base["capacity_hours"] > 0,
        ( base["forecast_hours"] / base["capacity_hours"] * 100).round(1),
        0,
    )
    #   base["load_ratio"] = np.where(
    #     base["capacity_hours"] > 0,
    #     ((base["backlog_hours"] + base["forecast_hours"]) / base["capacity_hours"] * 100).round(1),
    #     0,
    # )

    return base


def current_month_kpis(data: dict) -> dict:
    """KPIs for the current calendar month."""
    current = pd.Timestamp.today().strftime("%Y-%m")
    roll = monthly_rollup(data, current, current)

    if roll.empty:
        return _empty_kpis(current)

    r = roll.iloc[0]
    capacity = float(r["capacity_hours"])
    actual   = float(r["actual_hours"])
    backlog  = float(r["backlog_hours"])
    forecast = float(r["forecast_hours"])

    # Backlog order count
    bl = data["backlog"]
    bl_count = 0
    if not bl.empty and "month_period" in bl.columns:
        bl_count = int((bl["month_period"] == current).sum())

    # Forecast variance (actual vs forecast for current month)
    variance_pct = 0
    if forecast > 0:
        variance_pct = round((actual - forecast) / forecast * 100, 1)

    return {
        "month": current,
        "capacity_hours": round(capacity, 1),
        "actual_hours": round(actual, 1),
        "backlog_hours": round(backlog, 1),
        "forecast_hours": round(forecast, 1),
        "operational_efficiency": float(r["operational_efficiency"]),
        "load_ratio": float(r["load_ratio"]),
        "backlog_order_count": bl_count,
        "forecast_variance_pct": variance_pct,
        "capacity_remaining": round(max(capacity - actual, 0), 1),
    }


def _empty_kpis(month):
    return {
        "month": month,
        "capacity_hours": 0, "actual_hours": 0, "backlog_hours": 0,
        "forecast_hours": 0, "operational_efficiency": 0, "load_ratio": 0,
        "backlog_order_count": 0, "forecast_variance_pct": 0, "capacity_remaining": 0,
    }


def family_breakdown(data: dict, start: str, end: str) -> list:
    """Hours consumed by family_code in range. Returns list of dicts."""
    act = data["actuals"]
    if act.empty or "family_code" not in act.columns:
        return []

    mask = (act["month_period"] >= start) & (act["month_period"] <= end)
    sub = act[mask]
    if sub.empty:
        return []

    agg = sub.groupby("family_code", as_index=False).agg(
        hours=("hours_consumed", "sum"),
        quantity=("quantity", "sum"),
    )
    agg = agg.sort_values("hours", ascending=False)
    total = agg["hours"].sum()
    agg["pct"] = (agg["hours"] / total * 100).round(1) if total > 0 else 0
    agg["hours"] = agg["hours"].round(1)
    return agg.to_dict("records")


def top_items(data: dict, start: str, end: str, n: int = 10) -> list:
    """Top N items by quantity in range."""
    act = data["actuals"]
    if act.empty:
        return []

    mask = (act["month_period"] >= start) & (act["month_period"] <= end)
    sub = act[mask]
    if sub.empty:
        return []

    group_cols = ["item"]
    if "description" in sub.columns:
        group_cols.append("description")

    agg = sub.groupby(group_cols, as_index=False).agg(
        quantity=("quantity", "sum"),
        hours=("hours_consumed", "sum"),
    )
    agg = agg.sort_values("quantity", ascending=False).head(n)
    agg["hours"] = agg["hours"].round(1)
    return agg.to_dict("records")


def daily_breakdown(data: dict, month: str) -> pd.DataFrame:
    """Daily production for a specific month (YYYY-MM)."""
    act = data["actuals"]
    if act.empty or "transaction_date" not in act.columns:
        return pd.DataFrame(columns=["date", "quantity", "hours_consumed"])

    sub = act[act["month_period"] == month].copy()
    if sub.empty:
        return pd.DataFrame(columns=["date", "quantity", "hours_consumed"])

    sub["date"] = sub["transaction_date"].dt.strftime("%Y-%m-%d")
    daily = sub.groupby("date", as_index=False).agg(
        quantity=("quantity", "sum"),
        hours_consumed=("hours_consumed", "sum"),
    )
    daily["hours_consumed"] = daily["hours_consumed"].round(1)
    daily = daily.sort_values("date")
    return daily


def backlog_by_month(data: dict, start: str, end: str) -> pd.DataFrame:
    """Backlog hours and order counts per month."""
    months = get_month_range(start, end)
    base = pd.DataFrame({"month_period": months})

    bl = data["backlog"]
    if bl.empty or "month_period" not in bl.columns:
        base["backlog_hours"] = 0.0
        base["order_count"] = 0
        return base

    agg = bl.groupby("month_period", as_index=False).agg(
        backlog_hours=("total_labor_hours", "sum"),
        order_count=("ordernumber", "count") if "ordernumber" in bl.columns else ("item", "count"),
    )
    base = base.merge(agg, on="month_period", how="left")
    base["backlog_hours"] = base["backlog_hours"].fillna(0).round(1)
    base["order_count"] = base["order_count"].fillna(0).astype(int)
    return base


def forecast_vs_capacity(data: dict, start: str, end: str) -> pd.DataFrame:
    """Forecast hours vs capacity per month with efficiency ratio."""
    roll = monthly_rollup(data, start, end)
    out = roll[["month_period", "capacity_hours", "forecast_hours"]].copy()
    out["forecast_efficiency"] = np.where(
        out["capacity_hours"] > 0,
        (out["forecast_hours"] / out["capacity_hours"] * 100).round(1),
        0,
    )
    out["capacity_remaining"] = (out["capacity_hours"] - out["forecast_hours"]).round(1)
    return out
