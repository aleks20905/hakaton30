from flask import Flask, render_template, request, send_file, redirect, url_for, flash, abort
from functools import lru_cache
from datetime import datetime
import pandas as pd
import os

from analytics_helpers import (
    monthly_rollup,
    current_month_kpis,
    family_breakdown,
    top_items,
    daily_breakdown,
    backlog_by_month,
    forecast_vs_capacity,
    get_month_range,
    apply_family_filter,
    rebalance_rollup,
    movable_orders,
    _parse_overrides,
    _encode_overrides,
    _auto_rebalance,
    _backlog_with_effective_month,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-only-change-me")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
DATA_PATH = os.path.join(DATA_DIR, "Unified_Production_Analytics.xlsx")

os.makedirs(DATA_DIR, exist_ok=True)


# ==============================================================================
# DATA LOADING (cached by file mtime)
# ==============================================================================
@lru_cache(maxsize=1)
def _load_cached(mtime: float):
    """Load all sheets once per file version. mtime invalidates cache on upload."""
    sheets = pd.read_excel(DATA_PATH, sheet_name=None)

    # Normalize column names across all sheets
    for name, df in sheets.items():
        df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

    capacity = sheets.get("Capacity_Calendar", pd.DataFrame())
    backlog  = sheets.get("Demand_Backlog", pd.DataFrame())
    forecast = sheets.get("Production_Forecast", pd.DataFrame())
    actuals  = sheets.get("Production_Actuals", pd.DataFrame())
    items    = sheets.get("Item_Master", pd.DataFrame())

    # Parse dates defensively
    if "month" in capacity.columns:
        capacity["month"] = pd.to_datetime(capacity["month"], errors="coerce")
        capacity["month_period"] = capacity["month"].dt.strftime("%Y-%m")

    if "due_date" in backlog.columns:
        backlog["due_date"] = pd.to_datetime(backlog["due_date"], errors="coerce")
        backlog["month_period"] = backlog["due_date"].dt.strftime("%Y-%m")

    if "year_month" in forecast.columns:
        forecast["year_month"] = pd.to_datetime(forecast["year_month"], errors="coerce")
        forecast["month_period"] = forecast["year_month"].dt.strftime("%Y-%m")

    if "transaction_date" in actuals.columns:
        actuals["transaction_date"] = pd.to_datetime(actuals["transaction_date"], errors="coerce")
        actuals["month_period"] = actuals["transaction_date"].dt.strftime("%Y-%m")

    return {
        "capacity": capacity,
        "backlog": backlog,
        "forecast": forecast,
        "actuals": actuals,
        "items": items,
    }


def load_data():
    if not os.path.exists(DATA_PATH):
        return None
    try:
        return _load_cached(os.path.getmtime(DATA_PATH))
    except Exception as e:
        app.logger.error(f"Failed to load data: {e}")
        return None


# ==============================================================================
# REQUEST HELPERS
# ==============================================================================
def get_params(args):
    """Extract and normalize filter params from query string."""
    return {
        "start_month": args.get("start_month", "").strip(),
        "end_month":   args.get("end_month", "").strip(),
        "family":      args.get("family", "all").strip(),
        "item":        args.get("item", "").strip(),
        "quick":       args.get("quick", "").strip(),
    }


def resolve_date_range(params, data):
    """Resolve start_month/end_month, applying 'quick' shortcuts."""
    today = pd.Timestamp.today().normalize()
    this_month = today.strftime("%Y-%m")

    quick = params.get("quick", "")
    if quick == "this_month":
        return this_month, this_month
    if quick == "last_3":
        start = (today - pd.DateOffset(months=2)).strftime("%Y-%m")
        return start, this_month
    if quick == "next_3":
        end = (today + pd.DateOffset(months=3)).strftime("%Y-%m")
        return this_month, end
    if quick == "ytd":
        return f"{today.year}-01", this_month
    if quick == "next_6":
        end = (today + pd.DateOffset(months=6)).strftime("%Y-%m")
        return this_month, end

    # Fall back to explicit params or defaults from data
    start = params.get("start_month") or data_default_start(data)
    end   = params.get("end_month")   or data_default_end(data)
    return start, end


def data_default_start(data):
    """Default start = earliest actuals month, fall back to capacity."""
    if not data["actuals"].empty and "month_period" in data["actuals"]:
        vals = data["actuals"]["month_period"].dropna()
        if len(vals):
            return vals.min()
    if not data["capacity"].empty:
        return data["capacity"]["month_period"].min()
    return pd.Timestamp.today().strftime("%Y-%m")


def data_default_end(data):
    """Default end = latest forecast month, fall back to capacity."""
    if not data["forecast"].empty and "month_period" in data["forecast"]:
        vals = data["forecast"]["month_period"].dropna()
        if len(vals):
            return vals.max()
    if not data["capacity"].empty:
        return data["capacity"]["month_period"].max()
    return pd.Timestamp.today().strftime("%Y-%m")


def get_filter_options(data):
    """Build dropdown options for family filter."""
    families = set()
    for key in ("backlog", "forecast", "actuals"):
        df = data[key]
        if "family_code" in df.columns:
            families.update(df["family_code"].dropna().astype(str).unique())
    return {
        "families": sorted(families),
    }


def build_query_string(params):
    """Build URL query string from params for nav link preservation."""
    parts = []
    for k, v in params.items():
        if v and v != "all":
            parts.append(f"{k}={v}")
    return "&".join(parts)


# ==============================================================================
# ROUTES
# ==============================================================================
@app.route("/")
def index():
    data = load_data()
    if data is None:
        return render_template("waiting.html"), 503

    params = get_params(request.args)
    start, end = resolve_date_range(params, data)
    params["start_month"], params["end_month"] = start, end

    # Apply family filter
    filtered = apply_family_filter(data, params["family"])

    # Build data
    rollup = monthly_rollup(filtered, start, end)
    kpis = current_month_kpis(filtered)
    fam_mix = family_breakdown(filtered, start, end)
    top = top_items(filtered, start, end, n=10)

    options = get_filter_options(data)

    return render_template(
        "index.html",
        page="dashboard",
        params=params,
        options=options,
        filter_query=build_query_string(params),
        kpis=kpis,
        # Chart data
        months=rollup["month_period"].tolist(),
        capacity_hours=rollup["capacity_hours"].tolist(),
        actual_hours=rollup["actual_hours"].tolist(),
        backlog_hours=rollup["backlog_hours"].tolist(),
        forecast_hours=rollup["forecast_hours"].tolist(),
        op_eff=rollup["operational_efficiency"].tolist(),
        load_ratio=rollup["load_ratio"].tolist(),
        family_mix=fam_mix,
        top_items=top,
    )


@app.route("/capacity")
def capacity_view():
    data = load_data()
    if data is None:
        return render_template("waiting.html"), 503

    params = get_params(request.args)
    start, end = resolve_date_range(params, data)
    params["start_month"], params["end_month"] = start, end

    filtered = apply_family_filter(data, params["family"])
    rollup = monthly_rollup(filtered, start, end)
    options = get_filter_options(data)

    return render_template(
        "capacity.html",
        page="capacity",
        params=params,
        options=options,
        filter_query=build_query_string(params),
        rollup=rollup.to_dict("records"),
        months=rollup["month_period"].tolist(),
        capacity_hours=rollup["capacity_hours"].tolist(),
        actual_hours=rollup["actual_hours"].tolist(),
        backlog_hours=rollup["backlog_hours"].tolist(),
        forecast_hours=rollup["forecast_hours"].tolist(),
        op_eff=rollup["operational_efficiency"].tolist(),
        load_ratio=rollup["load_ratio"].tolist(),
    )


@app.route("/backlog")
def backlog_view():
    data = load_data()
    if data is None:
        return render_template("waiting.html"), 503

    params = get_params(request.args)
    start, end = resolve_date_range(params, data)
    params["start_month"], params["end_month"] = start, end

    filtered = apply_family_filter(data, params["family"])
    by_month = backlog_by_month(filtered, start, end)
    options = get_filter_options(data)

    # Top backlog orders
    bl = filtered["backlog"].copy()
    if "month_period" in bl.columns:
        bl = bl[(bl["month_period"] >= start) & (bl["month_period"] <= end)]
    top_orders = bl.nlargest(20, "total_labor_hours") if "total_labor_hours" in bl.columns else pd.DataFrame()

    return render_template(
        "backlog.html",
        page="backlog",
        params=params,
        options=options,
        filter_query=build_query_string(params),
        months=by_month["month_period"].tolist(),
        backlog_hours=by_month["backlog_hours"].tolist(),
        order_counts=by_month["order_count"].tolist(),
        top_orders=top_orders.to_dict("records"),
        total_orders=len(bl),
        total_hours=round(bl["total_labor_hours"].sum(), 1) if "total_labor_hours" in bl.columns else 0,
    )


@app.route("/forecast")
def forecast_view():
    data = load_data()
    if data is None:
        return render_template("waiting.html"), 503

    params = get_params(request.args)
    start, end = resolve_date_range(params, data)
    params["start_month"], params["end_month"] = start, end

    filtered = apply_family_filter(data, params["family"])
    fvc = forecast_vs_capacity(filtered, start, end)
    options = get_filter_options(data)

    # Summary stats for KPI cards
    effs = fvc["forecast_efficiency"].tolist()
    avg_eff = round(sum(effs) / len(effs), 1) if effs else 0
    months_over = sum(1 for e in effs if e >= 100)
    total_fc = int(fvc["forecast_hours"].sum())
    total_cap = int(fvc["capacity_hours"].sum())
    total_remaining = int(fvc["capacity_hours"].sum() - fvc["forecast_hours"].sum())

    return render_template(
        "forecast.html",
        page="forecast",
        params=params,
        options=options,
        filter_query=build_query_string(params),
        months=fvc["month_period"].tolist(),
        capacity_hours=fvc["capacity_hours"].tolist(),
        forecast_hours=fvc["forecast_hours"].tolist(),
        forecast_eff=fvc["forecast_efficiency"].tolist(),
        rows=fvc.to_dict("records"),
        avg_eff=avg_eff,
        months_over=months_over,
        total_months=len(effs),
        total_fc=total_fc,
        total_cap=total_cap,
        total_remaining=total_remaining,
    )


@app.route("/daily/<month>")
def daily_view(month):
    """Daily drill-down for a specific month (YYYY-MM)."""
    data = load_data()
    if data is None:
        return render_template("waiting.html"), 503

    # Validate month format
    try:
        datetime.strptime(month, "%Y-%m")
    except ValueError:
        abort(404)

    params = get_params(request.args)
    filtered = apply_family_filter(data, params["family"])
    daily = daily_breakdown(filtered, month)
    options = get_filter_options(data)

    return render_template(
        "daily.html",
        page="daily",
        month=month,
        params=params,
        options=options,
        filter_query=build_query_string(params),
        dates=daily["date"].tolist(),
        quantities=daily["quantity"].tolist(),
        hours=daily["hours_consumed"].tolist(),
        rows=daily.to_dict("records"),
    )
    
    
@app.route("/balancing")
def balancing_view():
    data = load_data()
    if data is None:
        return render_template("waiting.html"), 503

    params = get_params(request.args)
    start, end = resolve_date_range(params, data)
    params["start_month"], params["end_month"] = start, end

    filtered = apply_family_filter(data, params["family"])

    mode = request.args.get("mode", "manual")
    target = float(request.args.get("target", 90)) / 100.0
    moves_str = request.args.get("moves", "")
    overrides = _parse_overrides(moves_str)

    if mode == "auto":
        bl_eff = _backlog_with_effective_month(filtered, overrides)
        auto_moves = _auto_rebalance(
            bl_eff, filtered["capacity"], filtered["actuals"], target,
            forecast=filtered.get("forecast")
        )
        overrides.update(auto_moves)

    moves_str_with_auto = _encode_overrides(overrides)

    rollup = rebalance_rollup(filtered, start, end, overrides)
    orders = movable_orders(filtered, overrides)

    current = pd.Timestamp.today().strftime("%Y-%m")
    all_months = get_month_range(current, rollup["month_period"].max())
    options = get_filter_options(data)

    # Summary stats for KPI cards
    eff_before = rollup["efficiency_pct_original"].tolist()
    eff_after = rollup["efficiency_pct"].tolist()
    avg_before = round(sum(eff_before) / len(eff_before), 1) if eff_before else 0
    avg_after = round(sum(eff_after) / len(eff_after), 1) if eff_after else 0
    months_balanced = sum(1 for e in eff_after if 70 <= e <= 100)

    # ── BUILD KANBAN COLUMNS ───────────────────────────────────────────────
    import calendar
    import datetime as dt

    # if not orders.empty:
    #     print("ORDER COLUMNS:", orders.columns.tolist())
    orders_list = orders.to_dict("records") if not orders.empty else []

    kanban_cols = []
    for _, row in rollup.iterrows():
        m = row["month_period"]          # "YYYY-MM"
        year, mon = int(m[:4]), int(m[5:7])

        cal_weeks  = calendar.monthcalendar(year, mon)
        working_days = sum(1 for week in cal_weeks for d in week[:5] if d != 0)
        daily_cap  = round(row["capacity_hours"] / working_days, 1) if working_days else 0

        days = []
        num_days = calendar.monthrange(year, mon)[1]
        for d in range(1, num_days + 1):
            day_dt = dt.date(year, mon, d)
            if day_dt.weekday() < 5:          # Mon–Fri only
                days.append({
                    "date":          day_dt.strftime("%Y-%m-%d"),
                    "label":         day_dt.strftime("%a %-d"),   # "Mon 3"
                    "daily_capacity": daily_cap,
                })

        col_orders = [o for o in orders_list if o["effective_month"] == m]

        # Pre-compute values for the template to avoid Jinja2 inline issues
        cap = row["capacity_hours"]
        bl = row["backlog_hours"]
        pct = min(round((bl / cap * 100) if cap else 0, 1), 100)
        # Determine badge color class and text color
        if pct > 100:
            badge_class = "bg-red-500"
            text_color = "#fff"
        elif pct > 85:
            badge_class = "bg-yellow-500"
            text_color = "var(--text-primary)"
        else:
            badge_class = "bg-green-500"
            text_color = "var(--text-primary)"
        # Determine fill bar color
        if pct > 100:
            fill_color = "#ef4444"
        elif pct > 85:
            fill_color = "#f59e0b"
        else:
            fill_color = "#22c55e"

        kanban_cols.append({
            "month":          m,
            "capacity_hours": cap,
            "backlog_hours":  bl,
            "pct":            pct,
            "badge_class":    badge_class,
            "text_color":     text_color,
            "fill_color":     fill_color,
            "working_days":   working_days,
            "days":           days,
            "orders":         col_orders,
        })
    # ── END KANBAN BUILD ───────────────────────────────────────────────────
    
    return render_template(
        "balancing.html",
        page="balancing",
        params=params,
        options=options,
        mode=mode,
        target_pct=int(target * 100),
        moves_str=moves_str,
        moves_str_with_auto=moves_str_with_auto,  # mode switch button
        overrides=overrides,
        filter_query=build_query_string(params),
        months=rollup["month_period"].tolist(),
        capacity_hours=rollup["capacity_hours"].tolist(),
        actual_hours=rollup["actual_hours"].tolist(),
        backlog_hours=rollup["backlog_hours"].tolist(),
        backlog_hours_original=rollup["backlog_hours_original"].tolist(),
        efficiency_pct=rollup["efficiency_pct"].tolist(),
        efficiency_pct_original=rollup["efficiency_pct_original"].tolist(),                  # keep original for forms
        orders=orders_list,
        all_months=all_months,
        kanban_columns=kanban_cols,
        now_month=pd.Timestamp.today().strftime("%Y-%m"),
        avg_before=avg_before,
        avg_after=avg_after,
        months_balanced=months_balanced,
        total_months=len(eff_after),
    )

@app.route("/balancing/move", methods=["POST"])
def balancing_move():
    import json
    from urllib.parse import urlparse, parse_qs

    order        = request.form.get("order_number", "").strip()
    target_month = request.form.get("target_month", "").strip()
    moves_str    = request.form.get("moves", "")
    action       = request.form.get("action", "move")

    overrides = _parse_overrides(moves_str)

    if action == "clear_all":
        overrides = {}
    elif action == "reset" and order:
        overrides.pop(order, None)
    elif action == "move" and order and target_month:
        overrides[order] = target_month
    elif action == "bulk_move":
        bulk = json.loads(request.form.get("bulk_data", "{}"))
        for o_num, t_month in bulk.items():
            if o_num and t_month:
                overrides[o_num] = t_month

    # Parse query string from the page that submitted the form
    referer = request.headers.get("Referer", "")
    parsed  = urlparse(referer)
    qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}

    # Only overwrite moves
    qs["moves"] = _encode_overrides(overrides)

    return redirect(url_for("balancing_view", **qs))

# ==============================================================================
# UPLOAD / DOWNLOAD
# ==============================================================================
@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return redirect(request.referrer or url_for("index"))
    file = request.files["file"]
    if not file.filename.lower().endswith(".xlsx"):
        return redirect(request.referrer or url_for("index"))

    file.save(DATA_PATH)
    _load_cached.cache_clear()  # Invalidate cache
    return redirect(request.referrer or url_for("index"))


@app.route("/download")
def download():
    if not os.path.exists(DATA_PATH):
        abort(404)
    return send_file(DATA_PATH, as_attachment=True)


@app.route("/refresh")
def refresh():
    _load_cached.cache_clear()
    return redirect(request.referrer or url_for("index"))


if __name__ == "__main__":
    app.run(debug=os.environ.get("FLASK_DEBUG", "false").lower() == "true")
