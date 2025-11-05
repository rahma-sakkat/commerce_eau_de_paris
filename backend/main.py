import requests
import pandas as pd
from sqlalchemy import create_engine

# ---------- EXTRACT ----------
url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/dossiers-recents-durbanisme/records"
params = {
    "limit": 100,
    "offset": 0,
    "timezone": "Africa/Lagos"
}

resp = requests.get(url, params=params)
resp.raise_for_status()
data = resp.json()

records = data.get("results", [])
df = pd.DataFrame(records)
print(f"Total results returned: {len(df)}")

# ---------- TRANSFORM ----------
# 1) Normalize column names
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# 2) Drop unneeded columns
cols_to_drop = ["geo_shape", "geo_point_2d", "x", "y"]
df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

# 3) Parse dates
for c in ["date_depot", "date_decision"]:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")

# 4) Add 'annee' from date_depot
if "date_depot" in df.columns:
    df["annee"] = df["date_depot"].dt.year

# 5) Check and remove duplicates
dup_count = df.duplicated().sum()
print(f"\n=== Duplicate rows found: {dup_count} ===")
df = df.drop_duplicates()
print(f"Rows after removing duplicates: {len(df)}")

# 6) Missing values (excluding type_decision & date_decision)
allowed_null = {"type_decision", "date_decision"}
cols_for_missing = [c for c in df.columns if c not in allowed_null]

missing_counts = df[cols_for_missing].isna().sum()
missing_pct = (missing_counts / len(df) * 100).round(2)

missing_report = (
    pd.DataFrame({"missing_count": missing_counts, "missing_pct": missing_pct})
    .sort_values("missing_count", ascending=False)
)

print("\n=== Missing values per column (excluding: type_decision, date_decision) ===")
print(
    missing_report[missing_report["missing_count"] > 0]
    if missing_counts.sum() > 0
    else "No missing values in the checked columns."
)

print("\n=== HEAD (cleaned) ===")
print(df.head())

# ---------- LOAD (MySQL) ----------
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""   
MYSQL_DB = "eau_de_paris"

# Create SQLAlchemy connection string
conn_str = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"

# Create connection engine
engine = create_engine(conn_str)

# Load DataFrame into MySQL (replace table name as needed)
table_name = "dossiers_urbanisme"
df.to_sql(table_name, con=engine, if_exists="replace", index=False)

print(f"\n Data successfully loaded into MySQL table: {table_name} (database: {MYSQL_DB})")