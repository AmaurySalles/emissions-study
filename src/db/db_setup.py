import sqlite3
import logging
import pandas as pd
from typing import List, Dict, Callable
from enum import Enum

from src.utils.cleaning_utils import update_FR_region_names
from src.db.db_queries import upload_data

from src.db.db_tables import ALL_DB_TABLES

logging.basicConfig(level=logging.INFO)

def create_db(tables:List[Callable[[None], str]]=ALL_DB_TABLES) -> None:
    """
    Creating all dimension and fact DB tables.
    """
    
    db = sqlite3.connect('ecoact.db')
    c = db.cursor()

    for t in tables:
        try:
            c.execute(t())
            db.commit()
            logging.info(f'Created {t.__name__} table.')
        except sqlite3.OperationalError as e:
            logging.error(f'Skipped {e}.')
    
    db.close()

def init_db_dim_data() -> None:
    
    # Countries                                                         # Namibia's acronym is 'NA'
    countries = pd.read_csv('./data/countries.csv', encoding='latin-1', keep_default_na=False) 
    upload_data(countries, "Dim_Countries")
    
    # Regions
    fr_regions = pd.read_csv('./data/french_regions.csv', encoding='latin-1')
    fr_regions['country_iso_3'] = 'FRA'
    upload_data(fr_regions, "Dim_Regions")
    
    # Departments
    fr_dept_dims = prep_FR_dept_dim_data()
    upload_data(fr_dept_dims, "Dim_Departments")
    
    # Units - TODO: Clean unique units
    data = pd.read_csv('./data/donnees_candidats_dev_python.csv', encoding='latin-1')
    units = pd.DataFrame(data['Unité français'].unique()).rename(columns={0:'unit_name'})
    upload_data(units.astype(str), "Dim_Units")
    
    # Sources - TODO: Clean unique sources
    sources = pd.DataFrame(data['Source'].unique()).rename(columns={0:'source_name'}) # TODO: To add source_url if avaiable
    upload_data(sources.astype(str), "Dim_Sources")

    # Electricity mix source type
    elec_mix_source_types = pd.DataFrame({'post_type_name': ['Amont', 'Combustion à la centrale', 'Transport et distribution', 'Total']})
    upload_data(elec_mix_source_types, "Dim_Elec_mix_post_types")
    
    # Tree types
    tree_types = pd.DataFrame({'tree_type_name': ['Deciduous', 'Coniferous', 'Mixed', 'Total']})
    upload_data(tree_types, "Dim_Tree_types")
    
    # Forest types
    forest_types = pd.DataFrame({'forest_type_name': ['Open forest', 'Closed forest', 'Open and closed forest', 'Total']})
    upload_data(forest_types, "Dim_Forest_types")
    

def prep_FR_dept_dim_data() -> pd.DataFrame:
    """
    Retrieves all french departments and regions, alongside their corresponding ids
    -----------
    returns:
        df containing the relationship between each department, region and country
        for upload into the db.
    """
    regions = pd.read_csv('./data/french_regions.csv', encoding='latin-1')
    depts = pd.read_csv('./data/french_dept.csv', encoding='latin-1')

    depts['region'] = update_FR_region_names(depts['Région administrative'])
    depts['region_id'] = depts['region'].map(regions.set_index('region_name')['region_id']).astype(int)
    depts.rename(columns={'Département ':'dept_name'}, inplace=True)
    depts['dept_name'] = depts['dept_name'].str.strip()
    depts['dept_name'].replace({"Alpes de Haute-Provence": "Alpes-de-Haute-Provence",  # To match fr-geo-json file
                                "Ardêche": "Ardèche",
                                "Côtes d'Armor":"Côtes-d'Armor",
                                "Île-et-Vilaine":"Ille-et-Vilaine",
                                "Territoire-de-Belfort":"Territoire de Belfort"},
                            regex=True, inplace=True)
    depts['dept_id'] = depts['dept_id'].str.zfill(2) # Changes id from '1' to '01'
    dept_dims = depts[['dept_id', 'dept_name', 'region_id']].copy()
    
    return dept_dims

def retrieve_db_dim_data() -> Dict[str, Enum]:

    db = sqlite3.connect('ecoact.db')

    Countries = create_enum_from_table(db, 'Dim_Countries', 'iso_3', 'fr_country_name')
    Departments = create_enum_from_table(db, 'Dim_Departments', 'dept_id', 'dept_name')
    Regions = create_enum_from_table(db, 'Dim_Regions', 'region_id', 'region_name')
    Elec_mix_post_type = create_enum_from_table(db, 'Dim_Elec_mix_post_types', 'post_type_id', 'post_type_name')
    Sources = create_enum_from_table(db, 'Dim_Sources', 'source_id', 'source_name')
    Units = create_enum_from_table(db, 'Dim_Units', 'unit_id', 'unit_name')
    Tree_types = create_enum_from_table(db, 'Dim_Tree_types', 'tree_type_id', 'tree_type_name')
    Forest_types = create_enum_from_table(db, 'Dim_Forest_types', 'forest_type_id', 'forest_type_name')
    
    db_dims = {
        'Countries': Countries,
        'Regions': Regions,
        'Depts' : Departments,
        'Elec_mix_post_types': Elec_mix_post_type,
        'Sources': Sources,
        'Units': Units,
        'Forest_types': Forest_types,
        'Tree_types': Tree_types,
    }
    
    return db_dims

def create_enum_from_table(db: sqlite3.Connection, table_name: str, name_column: str, value_column: str) -> Enum:
    query = db.execute(f"SELECT {name_column}, {value_column} FROM {table_name}")
    result = dict(query.fetchall())
    result = {str(key): val for key, val in result.items()}
    dim_enum = Enum(table_name, result)
    return dim_enum


if __name__=="__main__":
    create_db()
    init_db_dim_data()
    retrieve_db_dim_data()
