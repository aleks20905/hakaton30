from flask import Flask, render_template, request, send_file, redirect
import pandas as pd
from calendar import monthrange
from datetime import datetime, timedelta
import os

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-only")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
DATA_PATH = os.path.join(DATA_DIR, "production_data.xlsx")
UPLOAD_FOLDER = "data"

def load_data():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError("production_data.xlsx not found in /data folder")
    dataframe = pd.read_excel(DATA_PATH)
    dataframe["date"] = pd.to_datetime(dataframe["date"])
    return dataframe

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() == "xlsx"

@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return "No file part", 400
    uploaded_file = request.files["file"]
    if uploaded_file.filename == "":
        return "No file selected", 400
    if not allowed_file(uploaded_file.filename):
        return "Only .xlsx files are allowed", 400
    
    temp_path = os.path.join(UPLOAD_FOLDER, "production_data_temp.xlsx")
    final_path = os.path.join(UPLOAD_FOLDER, "production_data.xlsx")
    
    uploaded_file.save(temp_path)
    
    try:
        test_dataframe = pd.read_excel(temp_path)
        required_columns = {"date", "planned", "actual", "defects"}
        if not required_columns.issubset(test_dataframe.columns):
            os.remove(temp_path)
            return f"Missing required columns: {required_columns - set(test_dataframe.columns)}", 400
    except Exception as error:
        os.remove(temp_path)
        return f"Could not read file: {error}", 400
    
    os.replace(temp_path, final_path)
    return redirect(request.referrer or "/")

@app.route("/download")
def download_file():
    if not os.path.exists(DATA_PATH):
        return "File not found", 404
    return send_file(DATA_PATH, as_attachment=True, download_name="production_data.xlsx")

def get_default_end():
    """Latest date in reasonable range"""
    return datetime.now().strftime("%Y-%m-%d")

def get_default_start_daily():
    """30 days back from today"""
    return (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

def get_params(arguments):
    range_type = arguments.get("range", "daily")

    start_date = arguments.get("start", "")
    end_date = arguments.get("end", "")
    start_month = arguments.get("start_month", "")
    end_month = arguments.get("end_month", "")

    has_any_parameter = any(key in arguments for key in ["range", "start", "end", "start_month", "end_month", "shift", "product", "view"])

    if range_type == "daily" and not start_date and not end_date and not has_any_parameter:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date and end_date and start_date > end_date:
        start_date, end_date = end_date, start_date
    if start_month and end_month and start_month > end_month:
        start_month, end_month = end_month, start_month

    return {
        "range_type": range_type,
        "start": start_date,
        "end": end_date,
        "start_month": start_month,
        "end_month": end_month,
        "shift": arguments.get("shift", "all"),
        "product": arguments.get("product", "all"),
        "view": arguments.get("view", "combined"),
    }

def apply_filters(dataframe, parameters):
    if parameters["start"]:
        dataframe = dataframe[dataframe["date"] >= pd.to_datetime(parameters["start"])]
    if parameters["end"]:
        dataframe = dataframe[dataframe["date"] <= pd.to_datetime(parameters["end"])]
    if parameters["start_month"]:
        dataframe = dataframe[dataframe["date"] >= pd.to_datetime(parameters["start_month"] + "-01")]
    if parameters["end_month"]:
        end_datetime = pd.to_datetime(parameters["end_month"] + "-01")
        last_day = monthrange(end_datetime.year, end_datetime.month)[1]
        dataframe = dataframe[dataframe["date"] <= end_datetime.replace(day=last_day)]
    if parameters["shift"] != "all":
        dataframe = dataframe[dataframe["shift"] == parameters["shift"]]
    if parameters["product"] != "all":
        dataframe = dataframe[dataframe["product"] == parameters["product"]]
    return dataframe

def group_data(dataframe, range_type, view="combined"):
    dataframe = dataframe.copy()

    if range_type == "daily":
        dataframe["period"] = dataframe["date"].dt.strftime("%Y-%m-%d")
    elif range_type == "weekly":
        dataframe["period"] = dataframe["date"].dt.to_period("W").apply(lambda week_range: week_range.start_time.strftime("%Y-%m-%d"))
    elif range_type == "monthly":
        dataframe["period"] = dataframe["date"].dt.strftime("%Y-%m")
    elif range_type == "quarterly":
        dataframe["period"] = dataframe["date"].dt.to_period("Q").astype(str)
    elif range_type == "yearly":
        dataframe["period"] = dataframe["date"].dt.strftime("%Y")
    else:
        dataframe["period"] = dataframe["date"].dt.strftime("%Y-%m-%d")

    group_columns = ["period"]
    if view == "per_shift":
        group_columns.append("shift")

    grouped_dataframe = dataframe.groupby(group_columns).agg(
        planned=("planned", "sum"),
        actual=("actual", "sum"),
        defects=("defects", "sum"),
        labor_hours=("labor_hours", "sum"),
        workers=("workers", "sum"),
    ).reset_index()

    grouped_dataframe["efficiency"] = grouped_dataframe.apply(
        lambda row: round((row["actual"] / row["planned"]) * 100, 1) if row["planned"] > 0 else 0, axis=1)
    grouped_dataframe["fpy"] = grouped_dataframe.apply(
        lambda row: round(((row["actual"] - row["defects"]) / row["actual"]) * 100, 1) if row["actual"] > 0 else 0, axis=1)
    grouped_dataframe["uplh"] = grouped_dataframe.apply(
        lambda row: round(row["actual"] / row["labor_hours"], 2) if row["labor_hours"] > 0 else 0, axis=1)
    grouped_dataframe["gap"] = grouped_dataframe["actual"] - grouped_dataframe["planned"]

    return grouped_dataframe

def get_bounds(dataframe):
    return {
        "min_date": dataframe["date"].min().strftime("%Y-%m-%d"),
        "max_date": dataframe["date"].max().strftime("%Y-%m-%d"),
        "min_month": dataframe["date"].min().strftime("%Y-%m"),
        "max_month": dataframe["date"].max().strftime("%Y-%m"),
        "products": sorted(dataframe[dataframe["product"] != "-"]["product"].unique().tolist()),
    }

def production_kpis(filtered_dataframe):
    production_data = filtered_dataframe[filtered_dataframe["status"] == "Production"]
    total_planned = int(production_data["planned"].sum())
    total_actual = int(production_data["actual"].sum())
    total_defects = int(production_data["defects"].sum())
    total_labor_hours = float(production_data["labor_hours"].sum())
    overall_efficiency = round((total_actual / total_planned) * 100, 1) if total_planned > 0 else 0
    overall_fpy = round(((total_actual - total_defects) / total_actual) * 100, 1) if total_actual > 0 else 0
    overall_uplh = round(total_actual / total_labor_hours, 2) if total_labor_hours > 0 else 0
    status_counts = filtered_dataframe["status"].value_counts().to_dict()
    return {
        "total_planned": total_planned,
        "total_actual": total_actual,
        "total_defects": total_defects,
        "total_labor_hours": total_labor_hours,
        "overall_eff": overall_efficiency,
        "overall_fpy": overall_fpy,
        "overall_uplh": overall_uplh,
        "status_counts": status_counts,
    }

@app.route("/")
def index():
    dataframe = load_data()
    parameters = get_params(request.args)
    bounds = get_bounds(dataframe)
    filtered_dataframe = apply_filters(dataframe, parameters)
    grouped_dataframe = group_data(filtered_dataframe, parameters["range_type"], parameters["view"])
    kpis = production_kpis(filtered_dataframe)

    return render_template("index.html",
        page="dashboard",
        params=parameters,
        bounds=bounds,
        grouped=grouped_dataframe.to_dict("records"),
        dates=grouped_dataframe["period"].tolist(),
        actuals=grouped_dataframe["actual"].tolist(),
        planned_list=grouped_dataframe["planned"].tolist(),
        defects=grouped_dataframe["defects"].tolist(),
        fpy_list=grouped_dataframe["fpy"].tolist(),
        uplh_list=grouped_dataframe["uplh"].tolist(),
        eff_list=grouped_dataframe["efficiency"].tolist(),
        **kpis,
    )

@app.route("/records")
def records():
    dataframe = load_data()
    parameters = get_params(request.args)
    bounds = get_bounds(dataframe)
    filtered_dataframe = apply_filters(dataframe, parameters)
    grouped_dataframe = group_data(filtered_dataframe, parameters["range_type"], parameters["view"])

    return render_template("records.html",
        page="records",
        params=parameters,
        bounds=bounds,
        data=grouped_dataframe.to_dict("records"),
        total=len(grouped_dataframe),
    )

@app.route("/analytics")
def analytics():
    dataframe = load_data()
    parameters = get_params(request.args)
    bounds = get_bounds(dataframe)
    filtered_dataframe = apply_filters(dataframe, parameters)
    grouped_dataframe = group_data(filtered_dataframe, parameters["range_type"], parameters["view"])
    kpis = production_kpis(filtered_dataframe)

    average_efficiency = round(grouped_dataframe["efficiency"].mean(), 1) if len(grouped_dataframe) else 0
    best_efficiency = round(grouped_dataframe["efficiency"].max(), 1) if len(grouped_dataframe) else 0
    worst_efficiency = round(grouped_dataframe["efficiency"].min(), 1) if len(grouped_dataframe) else 0
    average_fpy = round(grouped_dataframe["fpy"].mean(), 1) if len(grouped_dataframe) else 0
    average_uplh = round(grouped_dataframe["uplh"].mean(), 2) if len(grouped_dataframe) else 0

    return render_template("analytics.html",
        page="analytics",
        params=parameters,
        bounds=bounds,
        grouped=grouped_dataframe.to_dict("records"),
        dates=grouped_dataframe["period"].tolist(),
        efficiency=grouped_dataframe["efficiency"].tolist(),
        fpy_list=grouped_dataframe["fpy"].tolist(),
        defects=grouped_dataframe["defects"].tolist(),
        uplh_list=grouped_dataframe["uplh"].tolist(),
        eff_list=grouped_dataframe["efficiency"].tolist(),
        avg_eff=average_efficiency,
        best_eff=best_efficiency,
        worst_eff=worst_efficiency,
        avg_fpy=average_fpy,
        avg_uplh=average_uplh,
        **kpis,
    )

if __name__ == "__main__":
    app.run(debug=os.environ.get("FLASK_DEBUG", "false").lower() == "true")
