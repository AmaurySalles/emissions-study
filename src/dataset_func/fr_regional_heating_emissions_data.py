import pandas as pd
from enum import Enum
from typing import Dict

from src.utils.cleaning_utils import clean_FR_dates, update_FR_region_names
from src.db.db_queries import fetch_all_from_table, upload_data

DB_TABLE = 'FR_regional_heating_emissions'

def import_fr_regional_heating_emissions_data(full_dataset:pd.DataFrame, db_dims:Dict[str,Enum]) -> None:
    data = retrieve_fr_regional_heating_emissions_data(full_dataset)
    data = clean_fr_regional_heating_emissions_data(data)
    data = prep_fr_regional_heating_emissions_data(data, db_dims)
    upload_data(data, DB_TABLE)

def retrieve_fr_regional_heating_emissions_data(data:pd.DataFrame) -> pd.DataFrame:
    # Split main categories
    data[['Cat1', 'Cat2', 'Cat3', 'Cat4', 'Cat5', 'Cat6']] = data["Code de la catégorie"].str.split(" > ", expand=True)
    
    # FR regional heat data
    regional_heat_data = data[data['Cat1']=="Réseaux de chaleur / froid"].copy()
    
    return regional_heat_data

def clean_fr_regional_heating_emissions_data(data:pd.DataFrame) -> pd.DataFrame:
    clean_df = pd.DataFrame()    
    
    # Country
    clean_df['country'] = data['Localisation géographique'].apply(lambda x: x.split()[0])
    
    # Region
    data.loc[data["Sous-localisation géographique français"].isna(), "Sous-localisation géographique français"] = 'Corse, Corte' # From data-explore
    clean_df['region'] = data['Sous-localisation géographique français'].apply(lambda x: x.split(',')[0]) # Take only region-name
    clean_df['region'] = update_FR_region_names(clean_df['region'])
    
    # Split into dept, city and addresses
    clean_df[['dept_id', 'city', 'address', 'address2']] = data['Nom attribut français'].str.split(', ', expand=True)
    clean_df['dept_id'] = clean_df['dept_id'].str.zfill(2) # Changes id from '1' to '01'
    clean_df['address'] = clean_df['address'].str.cat(clean_df['address2'], sep=' ', na_rep='') # Concat & remove as only 2 val
    clean_df.drop(columns={'address2'}, inplace=True)
    
    
    # Heat_cycle (shorten value)
    clean_df['heat_cycle'] = data['Nom base français'].apply(lambda x: 'heat' if 'chaleur' in x else 'cool')
    
    # Format Dates
    clean_df['creation_date'] = clean_FR_dates(data['Date de création'], input_format='%B %Y')
    clean_df['modified_date'] = clean_FR_dates(data['Date de modification'], input_format='%B %Y')
    clean_df['validity_date'] = pd.to_datetime(data['Période de validité'].apply(lambda x: x.replace('Année ', '')), format='%Y')
    
    # Archive status
    # TODO: Create general 'status' function to check for other possible 'Status' values - only two values in this subset: ['Archivé' 'Valide générique']
    #       This can also be referring to a db_table dedicated to archive/live status
    clean_df['archived'] = data["Statut de l'élément"].apply(lambda x: True if x=='Archivé' else False)
    
    # Emmission values
    clean_df['emissions'] = data['Total poste non décomposé']
    dept_mean_emissions = clean_df[clean_df['emissions'] != 0].groupby('dept_id')['emissions'].mean() # TODO: Also filter by validity_date
    clean_df['emissions'] = clean_df.apply(lambda x: dept_mean_emissions[x['dept_id']] if x['emissions'] == 0 else x['emissions'], axis=1)
    
    # Uncertainty values
    clean_df['uncertainty'] = data['Incertitude']
    
    # Units
    clean_df['unit_name'] = data['Unité français']
    
    # Source
    clean_df['source_name'] = data['Source']
    
    return clean_df 

def prep_fr_regional_heating_emissions_data(data:pd.DataFrame, db_dims:Dict[str,Enum]) -> pd.DataFrame:
    """
    Given all required regional_heat_data as str, find the relevant foreign key ids from db.
    These functions make use of the DB dimension enums, and through a lambda function which 
    returns the enum name (table_id) for the first enum.value match.
    """
    # Find dept_ids
    # NA - Already within the dataset

    # Find unit_ids
    Units = db_dims['Units']
    data['unit_id'] = data['unit_name'].apply(lambda x: next((enum.name for enum in Units if enum.value == x), None))
    
    # Find source_ids
    Sources = db_dims['Sources']
    data['source_id'] = data['source_name'].apply(lambda x: next((enum.name for enum in Sources if enum.value == x), None)) 
    
    db_data = data[['dept_id', 'heat_cycle', 'emissions', 'unit_id','uncertainty', 
                    'creation_date','modified_date','validity_date', 'source_id']].copy()
    
    return db_data
