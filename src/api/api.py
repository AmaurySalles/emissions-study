from fastapi import FastAPI
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


app = FastAPI()
engine = create_engine("sqlite:///ecoact.db", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

## World Electricity mix emissions
@app.get("/world_elec_mix_emissions")
def get_world_electricity_mix_emissions():
    db = SessionLocal()

    try:  
        query = text("""SELECT json_object(
                        'Country', country_iso_3, 
                        'Emissions', emissions, 
                        'Date for', validity_date
                    ) 
                    FROM world_electricity_emissions
                    """)
        result = db.execute(query).fetchall()
    finally:
        db.close()

    if result:
        json_data = [json.loads(d[0]) for d in result]
        return json_data

@app.get("/world_elec_mix_emissions/{country_iso_3}")
def get_world_electricity_mix_emissions(country_iso_3: str):
    db = SessionLocal()

    try:  
        query = text("""SELECT json_object(
                            'Country', country_iso_3, 
                            'Emissions', emissions, 
                            'Date for', validity_date
                        ) 
                        FROM world_electricity_emissions
                        WHERE country_iso_3=:country_iso_3
                        """)
        result = db.execute(query, {"country_iso_3": country_iso_3}).fetchall()
    finally:
        db.close()

    if result:
        json_data = [json.loads(d[0]) for d in result]
        return json_data

## FR Regional heating emissions
@app.get("/fr_regional_heating_emissions")
def get_fr_regional_heating_emissions():
    db = SessionLocal()

    try:  
        query = text("""SELECT json_object(
                        'Department', dept_id, 
                        'Emissions', emissions, 
                        'Date for', validity_date
                    ) 
                    FROM fr_regional_heating_emissions
                    """)
        result = db.execute(query).fetchall()
    finally:
        db.close()

    if result:
        json_data = [json.loads(d[0]) for d in result]
        return json_data


@app.get("/fr_regional_heating_emissions/{dept_id}")
def get_fr_regional_heating_emissions_by_region(dept_id: str):
    
    db = SessionLocal()

    try:  
        query = text("""SELECT json_object(
                        'Department', dept_id, 
                        'Emissions', emissions, 
                        'Date for', validity_date
                    ) 
                    FROM fr_regional_heating_emissions 
                    WHERE dept_id=:dept_id
                    """)
        result = db.execute(query, {"dept_id": dept_id}).fetchall()
    finally:
        db.close()

    if result:
        json_data = [json.loads(d[0]) for d in result]
        return json_data

