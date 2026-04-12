import pandas as pd
import random
from datetime import datetime, timedelta

# ============ CONFIG ============
NUM_DAYS = 600
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=NUM_DAYS)

PRODUCTS = ["PCB-A", "PCB-B", "PCB-C"]

# Holidays (day, month)
HOLIDAYS = [
    (1, 1), (14, 1), (20, 3), (9, 4), (1, 5),
    (25, 7), (13, 8), (15, 10), (17, 12),
]

SHIFT_HOURS = 6 
WORKERS = {"Shift 1": 6, "Shift 2": 6}

# 2 maintenance days per month
random.seed(20905)
maintenance_days = set()
for month in range(1, 13):
    days_in_month = 28 if month == 2 else 30
    for d in random.sample(range(1, days_in_month + 1), 2):
        maintenance_days.add((d, month))


def is_holiday(date):
    return (date.day, date.month) in HOLIDAYS


def is_maintenance(date):
    return (date.day, date.month) in maintenance_days


def generate_shift(date, shift, product):
    workers = WORKERS[shift]
    labor_hours = workers * SHIFT_HOURS

    # Shift 1 slightly more productive
    if shift == "Shift 1":
        planned = random.randint(95, 135)
    else:
        planned = random.randint(80, 115)

    # Realistic actual: usually very close to planned
    roll = random.random()
    if roll < 0.70:
        actual = planned + random.randint(-3, 1)
    elif roll < 0.90:
        actual = planned + random.randint(2, 6)
    else:
        actual = planned - random.randint(5, 15)
    actual = max(0, actual)

    # Defects
    d_roll = random.random()
    if d_roll < 0.60:
        defects = random.randint(0, 2)
    elif d_roll < 0.85:
        defects = random.randint(3, 5)
    elif d_roll < 0.95:
        defects = random.randint(6, 10)
    else:
        defects = random.randint(11, 18)
    defects = min(defects, actual)

    fpy = round(((actual - defects) / actual) * 100, 1) if actual > 0 else 0.0
    uplh = round(actual / labor_hours, 2) if labor_hours > 0 else 0.0

    return {
        "date": date.strftime("%Y-%m-%d"),
        "shift": shift,
        "product": product,
        "workers": workers,
        "planned": planned,
        "actual": actual,
        "defects": defects,
        "fpy": fpy,
        "labor_hours": labor_hours,
        "uplh": uplh,
        "status": "Production"
    }


def generate_downtime(date, shift, reason):
    workers = WORKERS[shift]
    labor_hours = workers * SHIFT_HOURS if reason == "Maintenance" else 0

    return {
        "date": date.strftime("%Y-%m-%d"),
        "shift": shift,
        "product": "-",
        "workers": workers if reason == "Maintenance" else 0,
        "planned": 0,
        "actual": 0,
        "defects": 0,
        "fpy": 0.0,
        "labor_hours": labor_hours,
        "uplh": 0.0,
        "status": reason
    }


#  main ========================

rows = []

for i in range(NUM_DAYS):
    date = START_DATE + timedelta(days=i)
    if is_holiday(date):
        for s in ["Shift 1", "Shift 2"]:
            rows.append(generate_downtime(date, s, "Holiday"))
    elif is_maintenance(date):
        for s in ["Shift 1", "Shift 2"]:
            rows.append(generate_downtime(date, s, "Maintenance"))
    else:
        product = random.choice(PRODUCTS)
        for s in ["Shift 1", "Shift 2"]:
            rows.append(generate_shift(date, s, product))

df = pd.DataFrame(rows)
df.to_excel("data/production_data1.xlsx", index=False)

prod = df[df["status"] == "Production"]
print(f"Generated {len(df)} rows ({NUM_DAYS} days × 2 shifts)")
print(f"{df['date'].iloc[0]} → {df['date'].iloc[-1]}")
print(f"Production days: {len(prod) // 2}")
print(f"UPLH range: {prod['uplh'].min()} – {prod['uplh'].max()}")
print(f"FPY range: {prod['fpy'].min()}% – {prod['fpy'].max()}%")
print(f"\n{df['status'].value_counts().to_string()}")
