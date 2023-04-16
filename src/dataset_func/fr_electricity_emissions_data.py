import pandas as pd
from enum import Enum
from typing import Dict

from src.utils.cleaning_utils import clean_FR_dates
from src.db.db_queries import upload_data

DB_TABLE = 'World_electricity_emissions'

def import_fr_electricity_emissions_data(full_dataset:pd.DataFrame, db_dims:Dict[str,Enum]) -> None:
    data = retrieve_fr_electricity_emissions_data(full_dataset)
    data = clean_fr_electricity_emissions_data(data)
    data = prep_fr_electricity_emissions_data(data, db_dims)
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
    clean_df['post_type_name'] = data['Type poste'].fillna('Total')
    clean_df['unit_name'] = data['Unité français']
    clean_df['emissions'] = data['Total poste non décomposé']
    clean_df['source_name'] = data['Source']
    clean_df['uncertainty'] = data['Incertitude']
    clean_df['creation_date'] = clean_FR_dates(data['Date de modification'], input_format='%B %Y')
    clean_df['modified_date'] = clean_FR_dates(data['Date de modification'], input_format='%B %Y')
    
    return clean_df.sort_values('validity_date')

def prep_fr_electricity_emissions_data(data:pd.DataFrame, db_dims:Dict[str, Enum]) -> pd.DataFrame:
    """
    Given all required regional_heat_data as str, find the relevant foreign key ids from db.
    These functions make use of the DB dimension enums, and through a lambda function which 
    returns the enum name (table_id) for the first enum.value match.
    """
    # Find country_ids
    Countries = db_dims['Countries']
    data['country_iso_3'] = data['fr_country_name'].apply(lambda x: next((enum.name for enum in Countries if enum.value == x), None))
    
    # Find elec_mix_post_type_id
    Elec_mix_post_types = db_dims['Elec_mix_post_types']
    data['post_type_id'] = data['post_type_name'].apply(lambda x: next((enum.name for enum in Elec_mix_post_types if enum.value == x), None))

    
    # Find unit_ids
    Units = db_dims['Units']
    data['unit_id'] = data['unit_name'].apply(lambda x: next((enum.name for enum in Units if enum.value == x), None))
    
    # Find source_ids
    Sources = db_dims['Sources']
    data['source_id'] = data['source_name'].apply(lambda x: next((enum.name for enum in Sources if enum.value == x), None))
        
    db_data = data[['country_iso_3', 'post_type_id', 'emissions', 'unit_id','uncertainty', 
                    'creation_date','modified_date','validity_date', 'source_id']].copy()
    
    return db_data
