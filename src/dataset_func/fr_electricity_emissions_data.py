import pandas as pd

from src.utils.cleaning_utils import clean_FR_dates
from src.db.db_queries import fetch_all_from_table, upload_data

DB_TABLE = 'F_world_electricity_emissions'

def import_fr_electricity_emissions_data(full_dataset:pd.DataFrame) -> None:
    data = retrieve_fr_electricity_emissions_data(full_dataset)
    data = clean_fr_electricity_emissions_data(data)
    data = prep_fr_electricity_emissions_data(data)
    upload_data(data, DB_TABLE)

def retrieve_fr_electricity_emissions_data(data:pd.DataFrame) -> pd.DataFrame:
    # Split main categories
    data[['Cat1', 'Cat2', 'Cat3', 'Cat4', 'Cat5', 'Cat6']] = data["Code de la catégorie"].str.split(" > ", expand=True)
    
    # Find FR electricity mix data
    electricity_data = data[data['Cat1']=="Electricité"]
    elec_mix_data = electricity_data[electricity_data['Cat2']=="Mix réseau électrique"]
    electricity_data = data[data['Cat1']=="Electricité"]
    elec_mix_data = electricity_data[electricity_data['Cat2']=="Mix réseau électrique"]
    fr_elec_emissions_data = elec_mix_data[elec_mix_data['Cat3']=="France continentale"]
    fr_elec_emissions_data = fr_elec_emissions_data[fr_elec_emissions_data['Cat4']=="Moyen"]
    fr_elec_emissions_data = fr_elec_emissions_data[fr_elec_emissions_data['Nom attribut français']=='mix moyen'].copy()
    
    return fr_elec_emissions_data

def clean_fr_electricity_emissions_data(data:pd.DataFrame) -> pd.DataFrame:
    clean_df = pd.DataFrame()    
    
    clean_df['fr_country_name'] = data['Localisation géographique'].apply(lambda x: x.split(' ')[0])
    clean_df['validity_date'] = pd.to_datetime(data['Période de validité'].apply(lambda x: x.replace('Année ', '')), format='%Y')
    clean_df['source_type_name'] = data['Type poste'].fillna('Total')
    clean_df['unit_name'] = data['Unité français']
    clean_df['emissions'] = data['Total poste non décomposé']
    clean_df['source_name'] = data['Source']
    clean_df['uncertainty'] = data['Incertitude']
    clean_df['creation_date'] = clean_FR_dates(data['Date de modification'], input_format='%B %Y')
    clean_df['modified_date'] = clean_FR_dates(data['Date de modification'], input_format='%B %Y')
    
    return clean_df.sort_values('validity_date')

def prep_fr_electricity_emissions_data(data:pd.DataFrame) -> pd.DataFrame:
    """
    Given all required regional_heat_data as str, find the relevant foreign key ids from db.
    """
    # Find country_ids
    country_ids = fetch_all_from_table('Dim_Countries')
    data = data.merge(country_ids, on='fr_country_name', how='left')
    data.rename(columns={'id':'country_id'}, inplace=True)
    
    # Find elec_mix_source_type_id
    elec_mix_source_type_id = fetch_all_from_table('Dim_Elec_mix_source_types')
    data = data.merge(elec_mix_source_type_id, on='source_type_name', how='left')
    data.rename(columns={'id':'source_type_id'}, inplace=True)
    
    # Find unit_ids
    unit_ids = fetch_all_from_table('Dim_Units')
    data = data.merge(unit_ids, on='unit_name', how='left')
    data.rename(columns={'id':'unit_id'}, inplace=True)
    
    # Find source_ids
    source_ids = fetch_all_from_table('Dim_Sources')
    data = data.merge(source_ids, on='source_name', how='left')
    data.rename(columns={'id':'source_id'}, inplace=True)
        
    db_data = data[['country_id', 'source_type_id', 'emissions', 'unit_id','uncertainty', 
                    'creation_date','modified_date','validity_date', 'source_id']].copy()
    
    return db_data
