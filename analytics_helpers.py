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

# ==============================================================================
# BACKLOG REBALANCING (Pull-Forward)
# ==============================================================================

def _parse_overrides(moves_str: str) -> dict:
    """Parse 'ORDER1:2026-04,ORDER2:2026-05' → {'ORDER1': '2026-04', ...}"""
    if not moves_str:
        return {}
    out = {}
    for pair in moves_str.split(","):
        if ":" not in pair:
            continue
        order, month = pair.split(":", 1)
        order, month = order.strip(), month.strip()
        if order and month:
            out[order] = month
    return out


def _encode_overrides(overrides: dict) -> str:
    """Inverse of _parse_overrides — for building URLs."""
    return ",".join(f"{k}:{v}" for k, v in overrides.items())


def _backlog_with_effective_month(data: dict, overrides: dict) -> pd.DataFrame:
    """
    Returns backlog DataFrame with an 'effective_month' column.
    Invalid overrides (later than due_month) are silently dropped.
    """
    bl = data["backlog"].copy()
    if bl.empty or "order_number" not in bl.columns:
        return bl

    bl["due_month"] = bl["month_period"]  # original due month
    bl["effective_month"] = bl["due_month"]

    if overrides:
        # Apply override only if <= due_month
        for order, target_month in overrides.items():
            mask = bl["order_number"].astype(str) == str(order)
            if not mask.any():
                continue
            # Check each matching row — an order_number can appear multiple times
            for idx in bl[mask].index:
                due = bl.at[idx, "due_month"]
                if target_month <= due:  # string compare works for YYYY-MM
                    bl.at[idx, "effective_month"] = target_month

    return bl


def _auto_rebalance(bl: pd.DataFrame, capacity: pd.DataFrame,
                     actuals: pd.DataFrame, target: float,
                     forecast: pd.DataFrame = None, max_passes: int = 3) -> dict:
    """
    Forecast-aware multi-pass balancing:
    Phase 1: Pull orders forward to fill deficits (respecting due dates)
    Phase 2: Push surplus orders to future months with capacity
    Uses best-fit selection: picks orders whose size best matches the deficit.

    Returns overrides dict {order_number: new_month}.
    """
    if bl.empty or capacity.empty:
        return {}

    # Build month → capacity map
    cap_map = dict(zip(capacity["month_period"], capacity["available_gross_hours"]))

    # Build month → actual hours map
    act_map = {}
    if not actuals.empty and "hours_consumed" in actuals.columns:
        act_agg = actuals.groupby("month_period")["hours_consumed"].sum()
        act_map = act_agg.to_dict()

    # Build month → forecast hours map (for dynamic targets)
    fc_map = {}
    if forecast is not None and not forecast.empty and "required_labor_hours" in forecast.columns:
        fc_agg = forecast.groupby("month_period")["required_labor_hours"].sum()
        fc_map = fc_agg.to_dict()

    # Only consider future (or current) months — can't rebuild the past
    current = pd.Timestamp.today().strftime("%Y-%m")
    months_sorted = sorted(m for m in cap_map.keys() if m >= current)

    # Compute dynamic target per month: blend flat target with forecast-aware target
    # If forecast is high in a month, we can aim higher; if low, aim lower
    def get_dynamic_target(month, cap):
        base_target = target
        if fc_map and month in fc_map:
            fc_hrs = fc_map[month]
            # Forecast-informed target: blend configured target with forecast ratio
            fc_ratio = min(fc_hrs / cap, 1.0) if cap > 0 else 0
            # Use weighted average: 60% configured target, 40% forecast-informed
            dynamic = 0.6 * base_target + 0.4 * (0.7 + fc_ratio * 0.3)
            return min(dynamic, 1.0)
        return base_target

    all_months_seen = set(cap_map.keys()) | set(bl["due_month"].unique()) | set(bl["effective_month"].unique())
    load = {m: act_map.get(m, 0.0) for m in all_months_seen}
    for m in all_months_seen:
        load[m] += bl[bl["effective_month"] == m]["total_labor_hours"].sum()

    overrides = {}

    for _pass in range(max_passes):
        moved_this_pass = False

        # Phase 1: Fill deficits by pulling from later months
        for M in months_sorted:
            cap = cap_map.get(M, 0)
            if cap <= 0:
                continue
            dyn_target = get_dynamic_target(M, cap)
            deficit = dyn_target * cap - load[M]
            if deficit <= 1:
                continue

            # Candidate orders: due in a LATER month, not yet moved
            candidates = bl[
                (bl["due_month"] > M) &
                (bl["effective_month"] == bl["due_month"]) &
                (~bl["order_number"].isin(overrides.keys()))
            ].copy()

            if candidates.empty:
                continue

            # Best-fit: sort by how close order size is to deficit (ascending diff)
            candidates["size_diff"] = (candidates["total_labor_hours"] - deficit).abs()
            candidates = candidates.sort_values(
                ["size_diff", "due_month"], ascending=[True, True]
            )

            for _, row in candidates.iterrows():
                hrs = row["total_labor_hours"]
                if hrs > deficit:
                    continue
                order = row["order_number"]
                due = row["due_month"]
                overrides[order] = M
                load[M] = load.get(M, 0.0) + hrs
                load[due] = load.get(due, 0.0) - hrs
                deficit -= hrs
                moved_this_pass = True
                if deficit <= 1:
                    break

        # Phase 2: Push surplus orders to future months with capacity
        for M in reversed(months_sorted):  # Start from latest months
            cap = cap_map.get(M, 0)
            if cap <= 0:
                continue
            dyn_target = get_dynamic_target(M, cap)
            surplus = load[M] - dyn_target * cap
            if surplus <= 1:
                continue

            # Find future months with capacity
            for future_m in months_sorted:
                if future_m <= M:
                    continue
                future_cap = cap_map.get(future_m, 0)
                future_dyn_target = get_dynamic_target(future_m, future_cap)
                future_headroom = future_dyn_target * future_cap - load.get(future_m, 0.0)
                if future_headroom <= 1:
                    continue

                # Find orders in M that could be moved to future_m (must respect due date)
                pushable = bl[
                    (bl["effective_month"] == M) &
                    (bl["due_month"] >= future_m) &
                    (~bl["order_number"].isin(overrides.keys()))
                ].copy()

                if pushable.empty:
                    continue

                # Best-fit: pick orders that fit in the future headroom
                pushable["size_diff"] = (pushable["total_labor_hours"] - future_headroom).abs()
                pushable = pushable.sort_values(["size_diff", "due_month"], ascending=[True, True])

                for _, row in pushable.iterrows():
                    hrs = row["total_labor_hours"]
                    if hrs > future_headroom:
                        continue
                    order = row["order_number"]
                    overrides[order] = future_m
                    load[M] = load.get(M, 0.0) - hrs
                    load[future_m] = load.get(future_m, 0.0) + hrs
                    surplus -= hrs
                    future_headroom -= hrs
                    moved_this_pass = True
                    if surplus <= 1:
                        break

        # Stop if no moves were made in this pass
        if not moved_this_pass:
            break

    return overrides


def rebalance_rollup(data: dict, start: str, end: str,
                     overrides: dict) -> pd.DataFrame:
    """
    Monthly rollup after applying overrides — same shape as monthly_rollup()
    but backlog is grouped by effective_month instead of due_month.
    """
    months = get_month_range(start, end)
    base = pd.DataFrame({"month_period": months})

    # Capacity
    cap = data["capacity"]
    cap_col = "gross_hours" if "gross_hours" in cap.columns else "available_gross_hours"
    if not cap.empty and cap_col in cap.columns:
        cap_agg = cap[["month_period", cap_col]].rename(
            columns={cap_col: "capacity_hours"}
        )
        base = base.merge(cap_agg, on="month_period", how="left")
    else:
        base["capacity_hours"] = 0.0

    # Actuals (unchanged)
    act = data["actuals"]
    if not act.empty and "hours_consumed" in act.columns:
        act_agg = act.groupby("month_period", as_index=False)["hours_consumed"].sum()
        act_agg = act_agg.rename(columns={"hours_consumed": "actual_hours"})
        base = base.merge(act_agg, on="month_period", how="left")
    else:
        base["actual_hours"] = 0.0

    # Backlog by effective_month
    bl = _backlog_with_effective_month(data, overrides)
    if not bl.empty:
        bl_agg = bl.groupby("effective_month", as_index=False)["total_labor_hours"].sum()
        bl_agg = bl_agg.rename(columns={
            "effective_month": "month_period",
            "total_labor_hours": "backlog_hours"
        })
        base = base.merge(bl_agg, on="month_period", how="left")
    else:
        base["backlog_hours"] = 0.0

    # Also compute ORIGINAL backlog (before moves) for before/after chart
    if not bl.empty:
        orig_agg = bl.groupby("due_month", as_index=False)["total_labor_hours"].sum()
        orig_agg = orig_agg.rename(columns={
            "due_month": "month_period",
            "total_labor_hours": "backlog_hours_original"
        })
        base = base.merge(orig_agg, on="month_period", how="left")
    else:
        base["backlog_hours_original"] = 0.0

    for col in ["capacity_hours", "actual_hours", "backlog_hours", "backlog_hours_original"]:
        base[col] = base[col].fillna(0).round(1)

    # Forecast hours for comparison
    fc = data.get("forecast")
    if fc is not None and not fc.empty and "required_labor_hours" in fc.columns:
        fc_agg = fc.groupby("month_period", as_index=False)["required_labor_hours"].sum()
        fc_agg = fc_agg.rename(columns={"required_labor_hours": "forecast_hours"})
        base = base.merge(fc_agg, on="month_period", how="left")
    else:
        base["forecast_hours"] = 0.0

    base["forecast_hours"] = base["forecast_hours"].fillna(0).round(1)

    base["total_load"] = (base["actual_hours"] + base["backlog_hours"]).round(1)
    base["total_load_original"] = (base["actual_hours"] + base["backlog_hours_original"]).round(1)

    base["efficiency_pct"] = np.where(
        base["capacity_hours"] > 0,
        (base["total_load"] / base["capacity_hours"] * 100).round(1),
        0,
    )
    base["efficiency_pct_original"] = np.where(
        base["capacity_hours"] > 0,
        (base["total_load_original"] / base["capacity_hours"] * 100).round(1),
        0,
    )
    base["forecast_efficiency"] = np.where(
        base["capacity_hours"] > 0,
        (base["forecast_hours"] / base["capacity_hours"] * 100).round(1),
        0,
    )

    return base


def movable_orders(data: dict, overrides: dict) -> pd.DataFrame:
    """
    Return all backlog orders with their current effective_month and a list
    of valid target months (= due_month or earlier, but >= current month).
    Used to populate the manual-move table.
    """
    bl = _backlog_with_effective_month(data, overrides)
    if bl.empty:
        return bl

    current = pd.Timestamp.today().strftime("%Y-%m")

    # Sort by due date then hours descending
    bl = bl.sort_values(["due_month", "total_labor_hours"], ascending=[True, False])

    # Flag whether it's been moved
    bl["is_moved"] = bl["effective_month"] != bl["due_month"]
    bl["months_pulled"] = bl.apply(
        lambda r: (pd.Period(r["due_month"]) - pd.Period(r["effective_month"])).n
        if r["is_moved"] else 0,
        axis=1
    )

    return bl
