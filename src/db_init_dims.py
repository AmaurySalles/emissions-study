import pandas as pd
import sqlite3
import logging


def init_db_dim_data() -> None:
    # Countries                                                         # Namibia's acronym is 'NA'
    countries = pd.read_csv('./data/countries.csv', encoding='latin-1', keep_default_na=False) 
    upload_init_dim_data(countries, "Dim_Countries")
    
    # Regions
    fr_regions = pd.read_csv('./data/french_regions.csv', encoding='latin-1')
    fr_regions['country_id'] = 1  # Append FR country id
    upload_init_dim_data(fr_regions, "Dim_Regions")
    
    # Departments
    fr_dept_dims = fetch_FR_dept_dim_data()
    upload_init_dim_data(fr_dept_dims, "Dim_Departments")
    
    # Units
    data = pd.read_csv('./data/donnees_candidats_dev_python.csv', encoding='latin-1')
    units = pd.DataFrame(data['Unité français'].unique()).rename(columns={0:'unit_name'})
    upload_init_dim_data(units.astype(str), "Dim_Units")
    
    # Sources
    sources = pd.DataFrame(data['Source'].unique()).rename(columns={0:'source_name'}) # TODO: To add source_url if avaiable
    upload_init_dim_data(sources.astype(str), "Dim_Sources")


def upload_init_dim_data(geo_dim:pd.DataFrame, table:str) -> None:
    """
    Creating all dimension and fact DB tables.
    """
    db = sqlite3.connect('ecoact.db')

    try:
        geo_dim.to_sql(table, db, if_exists="append", index=False)
        logging.info(f'Uploaded init data to {table}.')
    except sqlite3.OperationalError as e:
        logging.error(e)


def fetch_FR_dept_dim_data() -> pd.DataFrame:
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

def update_FR_region_names(data:pd.Series) -> pd.Series:
    region_name_map = {
        "Alsace" : "Grand Est",
        "Champagne-Ardenne" : "Grand Est",
        "Lorraine": "Grand Est",
        "Aquitaine": "Aquitaine-Limousin-Poitou-Charentes",
        "Limousin": "Aquitaine-Limousin-Poitou-Charentes",
        "Poitou-Charentes": "Aquitaine-Limousin-Poitou-Charentes",
        "Auvergne": "Auvergne-Rhône-Alpes",
        "Rhône-Alpes": "Auvergne-Rhône-Alpes",
        "Bourgogne" : "Bourgogne-Franche-Comté Dijon",
        "Franche-Comté": "Bourgogne-Franche-Comté Dijon",
        "Centre-Val de Loire": "Centre",
        "Languedoc-Roussillon": "Occitanie",
        "Midi-Pyrénées":"Occitanie",
        "Nord-Pas-de-Calais": "Hauts de France",
        "Picardie":"Hauts de France",
        "Basse-Normandie": "Normandie",
        "Haute-Normandie": "Normandie",
    }
    data.replace(region_name_map, regex=True, inplace=True)
    
    return data.str.strip()

if __name__=="__main__":
    init_db_dim_data()
