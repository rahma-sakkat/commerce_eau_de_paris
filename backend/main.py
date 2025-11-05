import os
import requests
import pandas as pd
from fastapi import FastAPI, Depends
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

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
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
cols_to_drop = ["geo_shape", "geo_point_2d", "x", "y"]
df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

for c in ["date_depot", "date_decision"]:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")

if "date_depot" in df.columns:
    df["annee"] = df["date_depot"].dt.year

df = df.drop_duplicates()

# ---------- LOAD (MySQL) ----------
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_DB = "eau_de_paris"

# ✅ Create engine FIRST
engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}")

table_name = "dossiers_urbanisme"
df.to_sql(table_name, con=engine, if_exists="replace", index=False)
print(f"\nData loaded into MySQL table: {table_name}")

# ✅ THEN create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ✅ Define get_db()
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- FASTAPI APP ----------
app = FastAPI()

@app.get("/api/stats/annee")
def get_stats_by_year(db: Session = Depends(get_db)):
    query = text("""
        SELECT annee, COUNT(*) as total
        FROM dossiers_urbanisme
        WHERE annee IS NOT NULL
        GROUP BY annee
        ORDER BY annee
    """)
    result = db.execute(query).mappings().all()
    return result


@app.get("/api/stats/decision-pie")
def get_decision_pie():
    query = """
        SELECT type_decision, COUNT(*) as total
        FROM dossiers_urbanisme
        WHERE type_decision IS NOT NULL
        GROUP BY type_decision
    """
    df = pd.read_sql(query, con=engine)
    return df.to_dict(orient="records")

# ---------- FRONTEND ----------
frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
