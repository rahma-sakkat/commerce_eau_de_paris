import os
import requests
import pandas as pd
from fastapi import FastAPI, Depends
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

# ---------- EXTRACT PHASE----------
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

# ---------- TRANSFORM  PHASE----------
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
cols_to_drop = ["geo_shape", "geo_point_2d", "x", "y"]
df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

for c in ["date_depot", "date_decision"]:
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")

#  Add 'duree' column (in days)
if all(col in df.columns for col in ["date_depot", "date_decision"]):
    df["duree"] = (df["date_decision"] - df["date_depot"]).dt.days


if "date_depot" in df.columns:
    df["annee"] = df["date_depot"].dt.year
#removing duplicates
df = df.drop_duplicates()

# ---------- LOAD PHASE (MySQL) ----------

engine = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}")

table_name = "dossiers_urbanisme"
df.to_sql(table_name, con=engine, if_exists="replace", index=False)
print(f"\nData loaded into MySQL table: {table_name}")


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- FASTAPI APP ----------
app = FastAPI()
#it's an api that will return the count of each etat for each annee(column generated while transform phase)
@app.get("/api/stats/annee")
def get_stats_by_year(db: Session = Depends(get_db)):
    query = text("""
        SELECT annee, etat, COUNT(*) as total
        FROM dossiers_urbanisme
        WHERE annee IS NOT NULL AND etat IS NOT NULL
        GROUP BY annee, etat
        ORDER BY annee, etat
    """)
    result = db.execute(query).mappings().all()
    return result

#it's an api that will return the count of each type_decision
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

#it's an api that will return the count of each type_dossier for each etat
@app.get("/api/stats/type-dossier-etat")
def get_stats_by_type_etat(db: Session = Depends(get_db)):
    query = text("""
        SELECT type_dossier, etat, COUNT(*) as total
        FROM dossiers_urbanisme
        WHERE type_dossier IS NOT NULL AND etat IS NOT NULL
        GROUP BY type_dossier, etat
        ORDER BY type_dossier, etat
    """)
    result = db.execute(query).mappings().all()
    return result
#it's an api that will return the average duration (duree)(a column generated while transform pphase) for each type_dossier and etat
@app.get("/api/stats/duree-type-etat")
def get_duree_by_type_and_etat(db: Session = Depends(get_db)):
    query = text("""
        SELECT 
            type_dossier,
            etat,
            AVG(duree) AS duree_moyenne
        FROM dossiers_urbanisme
        WHERE duree IS NOT NULL 
          AND type_dossier IS NOT NULL
          AND etat IS NOT NULL
        GROUP BY type_dossier, etat
        ORDER BY type_dossier, etat
    """)
    result = db.execute(query).mappings().all()
    return result
#it's an api that will return the count of each etat for each circonscription
@app.get("/api/stats/circonscription-etat")
def get_circonscription_etat_stats(db: Session = Depends(get_db)):
    query = text("""
        SELECT circonscription, etat, COUNT(*) as total
        FROM dossiers_urbanisme
        WHERE circonscription IS NOT NULL AND etat IS NOT NULL
        GROUP BY circonscription, etat
        ORDER BY circonscription, etat
    """)
    result = db.execute(query).mappings().all()
    return result



#it's an api that will return the count of each type_decision for each etat
@app.get("/api/stats/etat-types")
def get_decision_type_stats(db: Session = Depends(get_db)):
    query = text("""
        SELECT etat, type_decision, COUNT(*) as total
        FROM dossiers_urbanisme
        WHERE etat IS NOT NULL AND type_decision IS NOT NULL
        GROUP BY etat, type_decision
        ORDER BY etat, type_decision
    """)
    result = db.execute(query).mappings().all()
    return result

# ---------- FRONTEND ----------
frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
