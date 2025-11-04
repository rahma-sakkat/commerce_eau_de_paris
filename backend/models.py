from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
from sqlalchemy import Column, Integer, String, Float, JSON
from database import Base


DATABASE_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Commerce(Base):
    __tablename__ = "commerces"

    id = Column(Integer, primary_key=True, index=True)
    nom_commerce = Column(String(255))
    num_voie = Column(String(255))
    nom_voie = Column(String(50))
    code_postal = Column(String(255))
    nom_commune = Column(String(255))
    latitude = Column(Float)
    longitude = Column(Float)
    geoshape_type = Column(String(50))           
    geoshape_geometry_coordinates_longitude = Column(Float)      # → "Point"
    geoshape_geometry_coordinates_latitude = Column(Float) 
    geoshape_geometry_type = Column(String(50)) 
    geoshape_properties = Column(String(255)) 
    geo_point_2d_geo_latitude = Column(Float)  
    geo_point_2d_geo_longitude = Column(Float) 
# Create table automatically
Base.metadata.create_all(bind=engine)
