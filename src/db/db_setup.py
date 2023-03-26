import sqlite3
import logging
import pandas as pd
from typing import List, Callable

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
    fr_regions['country_id'] = 1  # Append FR country_id - TODO: look-up from db / cheated here by re-placing it first in list.
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
    elec_mix_source_types = pd.DataFrame({'source_type_name': ['Amont', 'Combustion à la centrale', 'Transport et distribution', 'Total']})
    upload_data(elec_mix_source_types, "Dim_Elec_mix_source_types")

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
    dept_dims = depts[['dept_id', 'Département ', 'region_id']].copy()
    dept_dims = dept_dims.rename(columns={'Département ':'dept_name'})
    
    return dept_dims

if __name__=="__main__":
    create_db()
    init_db_dim_data()
