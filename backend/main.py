from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
import requests
from database import SessionLocal, engine
from models import Commerce, Base

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI()

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def home():
    return {"message": "API Eau de Paris is running!"}


@app.get("/load-data")
def load_data(db: Session = Depends(get_db)):
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/commerces-eau-de-paris/records?limit=100"
    response = requests.get(url)  # ✅ remove limits argument (was invalid)
    data = response.json().get("results", [])

    for item in data:
        geo_shape = item.get("geo_shape", {})
        geometry = geo_shape.get("geometry", {})
        coords = geometry.get("coordinates", [None, None])

        commerce = Commerce(
            nom_commerce=item.get("nom_commerce"),
            num_voie=item.get("num_voie"),
            nom_voie=item.get("nom_voie"),
            code_postal=item.get("code_postal"),
            nom_commune=item.get("nom_commune"),
            latitude=item.get("geo_point_2d", {}).get("lat") or item.get("lat"),
            longitude=item.get("geo_point_2d", {}).get("lon") or item.get("lon"),

            # Geo shape fields
            geoshape_type=geo_shape.get("type"),
            geoshape_geometry_type=geometry.get("type"),
            geoshape_geometry_coordinates_longitude=str(coords[0]),
            geoshape_geometry_coordinates_latitude=str(coords[1]),
            geoshape_properties=str(geo_shape.get("properties", {})),

            # Geo point fields
            geo_point_2d_geo_latitude=item.get("geo_point_2d", {}).get("lat"),
            geo_point_2d_geo_longitude=item.get("geo_point_2d", {}).get("lon"),
        )

        db.add(commerce)

    db.commit()  # ✅ Must be indented inside the function

    return {"message": f"{len(data)} records inserted successfully."}
