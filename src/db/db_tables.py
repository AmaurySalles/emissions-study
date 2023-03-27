"""
This file contains the queries to create each db tables.
Also contains a list, ALL_DB_TABLES, of callable functions,
to execute each table on db setup (see db_setup.py).
"""

def Dim_Countries() -> str:
    return """CREATE TABLE Dim_Countries (
                iso_3 TEXT NOT NULL UNIQUE PRIMARY KEY, 
                iso_2 TEXT NOT NULL UNIQUE,
                iso_n INTEGER NOT NULL UNIQUE,
                fr_country_name TEXT NOT NULL UNIQUE
            );
            """

def Dim_Regions() -> str:
    return """CREATE TABLE Dim_Regions (
                region_id INTEGER PRIMARY KEY,
                region_name TEXT NOT NULL UNIQUE,
                country_iso_3 TEXT NOT NULL,
                FOREIGN KEY (country_iso_3) REFERENCES Dim_Countries (iso_3)
            );
            """

def Dim_Departments() -> str:
    # dept_id must be text rather than integer, as Corsica is split into 20A and 20B
    return """CREATE TABLE Dim_Departments (
                dept_id TEXT PRIMARY KEY,
                dept_name TEXT NOT NULL UNIQUE,
                region_id INTEGER NOT NULL,
                FOREIGN KEY (region_id) REFERENCES Dim_Regions (region_id)
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

def Dim_Elec_mix_post_types() -> str:
    return """CREATE TABLE Dim_Elec_mix_post_types (
                post_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_type_name TEXT NOT NULL UNIQUE
            );
            """

def FR_regional_heating_emissions() -> str:
    # TODO: Add unique feature to differentiate whether data is already in db.
    return """ CREATE TABLE FR_regional_heating_emissions (
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
                FOREIGN KEY (dept_id) REFERENCES Dim_Departments (dept_id),
                FOREIGN KEY (unit_id) REFERENCES Dim_Units (unit_id),
                FOREIGN KEY (source_id) REFERENCES Dim_Sources (source_id)
            );
        """

def World_electricity_emissions() -> str:
    return """ CREATE TABLE World_electricity_emissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_iso_3 TEXT NOT NULL,
            validity_date DATE NOT NULL,
            post_type_id INTEGER,
            emissions REAL NOT NULL,
            unit_id INT NOT NULL,
            uncertainty REAL,
            creation_date DATE NOT NULL,
            modified_date DATE,
            source_id INTEGER,
            FOREIGN KEY (country_iso_3) REFERENCES Dim_Countries (iso_3),
            FOREIGN KEY (post_type_id) REFERENCES Dim_Elec_mix_post_types (post_type_id),
            FOREIGN KEY (unit_id) REFERENCES Dim_Units (unit_id),
            FOREIGN KEY (source_id) REFERENCES Dim_Sources (source_id)
        );
    """

# List of callable functions
ALL_DB_TABLES = [
    Dim_Countries,
    Dim_Regions,
    Dim_Departments,
    Dim_Units,
    Dim_Sources,
    Dim_Elec_mix_post_types,
    FR_regional_heating_emissions,
    World_electricity_emissions,
]
