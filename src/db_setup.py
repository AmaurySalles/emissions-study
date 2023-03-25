import sqlite3
import logging

from db_init_dims import init_db_dim_data

logging.basicConfig(level=logging.INFO)

def create_db() -> None:
    """
    Creating all dimension and fact DB tables.
    """
    db = sqlite3.connect('ecoact.db')
    c = db.cursor()

    tables = [
        Dim_Countries,
        Dim_Regions,
        Dim_Departments,
        Dim_Units,
        Dim_Sources,
        F_Regional_heat_emissions
    ]

    for t in tables:
        try:
            c.execute(t())
            db.commit()
            logging.info(f'Created {t.__name__} table.')
        except sqlite3.OperationalError as e:
            logging.error(f'Skipped {e}.')
    
    db.close()

def Dim_Countries() -> str:
    return """CREATE TABLE Dim_Countries (
                country_id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_name TEXT NOT NULL UNIQUE,
                country_short TEXT NOT NULL UNIQUE
            );
            """

def Dim_Regions() -> str:
    return """CREATE TABLE Dim_Regions (
                region_id INTEGER PRIMARY KEY,
                region_name TEXT NOT NULL UNIQUE,
                country_id INTEGER NOT NULL,
                FOREIGN KEY (country_id) REFERENCES countries (country_id)
            );
            """

def Dim_Departments() -> str:
    # dept_id must be text rather than integer, as Corsica is split into 20A and 20B
    return """CREATE TABLE Dim_Departments (
                dept_id TEXT PRIMARY KEY,
                dept_name TEXT NOT NULL UNIQUE,
                region_id INTEGER NOT NULL,
                FOREIGN KEY (region_id) REFERENCES regions (region_id)
            );
            """

def Dim_Units() -> str:
    return """CREATE TABLE Dim_Units (
                unit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                unit_name TEXT NOT NULL UNIQUE
            );
            """

def Dim_Sources() -> str:
    return """CREATE TABLE Dim_Sources (
                source_id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL UNIQUE,
                source_url TEXT UNIQUE
            );
            """
 
def F_Regional_heat_emissions() -> str:
    return """ CREATE TABLE F_Regional_heat_emissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dept_id TEXT NOT NULL,
                heat_cycle TEXT NOT NULL,
                emissions REAL NOT NULL,
                unit_id INT NOT NULL,
                uncertainty REAL,
                creation_date DATE NOT NULL,
                modified_date DATE,
                validity_date DATE NOT NULL,
                source_id INTEGER,
                FOREIGN KEY (dept_id) REFERENCES D_departments (dept_id),
                FOREIGN KEY (unit_id) REFERENCES D_units (unit_id)
                FOREIGN KEY (source_id) REFERENCES D_sources (source_id)
            );
        """


if __name__=="__main__":
    create_db()
    init_db_dim_data()