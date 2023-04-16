import pandas as pd
from enum import Enum
from typing import Dict

from src.utils.cleaning_utils import clean_FR_dates, update_FR_region_names
from src.db.db_queries import fetch_all_from_table, upload_data

DB_TABLE = 'FR_forestry_area'

def import_FR_forestry_area_data(full_dataset:pd.DataFrame, db_dims:Dict[str,Enum]) -> None:
    data = retrieve_FR_forestry_area_data(full_dataset)
    data = clean_FR_forestry_area_data(data)
    data = prep_FR_forestry_area_data(data, db_dims)
    upload_data(data, DB_TABLE)

def retrieve_FR_forestry_area_data(data:pd.DataFrame) -> pd.DataFrame:
    # Split main categories
    data[['Cat1', 'Cat2', 'Cat3', 'Cat4', 'Cat5', 'Cat6']] = data["Code de la catégorie"].str.split(" > ", expand=True)
    
    # FR regional forestry area data
    sliced_df = data[data['Cat2']=="Forêts françaises"].copy()
    sliced_df.dropna(axis=1, how='all', inplace=True)
    
    sliced_df = sliced_df.drop(columns=["Type Ligne", "Identifiant de l'élément", "Structure", "Statut de l'élément",
                                        "Code de la catégorie", "Programme", "Url du programme", "Transparence",
                                        "Cat1", "Cat2", "Qualité", "Qualité TeR", "Qualité GR", "Qualité TiR",
                                        "Qualité C", "Qualité P", "Qualité M"])
    
    return sliced_df

def clean_FR_forestry_area_data(data:pd.DataFrame) -> pd.DataFrame():
    clean_df = pd.DataFrame()

    clean_df['region_name'] = data['Nom frontière français']
    clean_df['region_name'] = update_FR_region_names(clean_df['region_name'])
    clean_df = clean_df[clean_df["region_name"] != "France"] # Remove country from dataset (only FR regions)
    
    clean_df['tree_type_name'] = data['Nom attribut français'].replace({'Conifère':"Coniferous",
                                                                        'Feuillu': "Deciduous",
                                                                        'Mixte': "Mixed",
                                                                        'Toutes compositions': "Mixed"})
    
    clean_df['forest_type_name'] = data['Nom base français'].replace({'Forêts fermées': 'Closed forest',
                                                                      'Forêts fermées et ouvertes': 'Open and closed forest'})
    
    clean_df['land_loss'] = data['Total poste non décomposé']
    clean_df['unit_name'] = data['Unité français']
    clean_df['uncertainty'] = data['Incertitude']
        
    # Format Dates
    clean_df['creation_date'] = clean_FR_dates(data['Date de création'], input_format='%B %Y')
    clean_df['modified_date'] = clean_FR_dates(data['Date de modification'], input_format='%B %Y')
    clean_df['validity_date'] = pd.to_datetime(data['Période de validité'], format='%d/%M/%Y')
    clean_df['validity_date'] = pd.to_datetime('2023-01-01') # Overwrite to format ('2023-01-31T00:12:00.000000000')
    
    clean_df['source_name'] = data['Source']
    
    return clean_df

def prep_FR_forestry_area_data(data:pd.DataFrame, db_dims:Dict[str,Enum]) -> pd.DataFrame:
    """
    Given all required regional_heat_data as str, find the relevant foreign key ids from db.
    These functions make use of the DB dimension enums, and through a lambda function which 
    returns the enum name (table_id) for the first enum.value match.
    """
    # Find dept_ids
    Regions = db_dims['Regions']
    data['region_id'] = data['region_name'].apply(lambda x: next((enum.name for enum in Regions if enum.value == x), None))
    
    # Find forest_type_id
    Forest_types = db_dims['Forest_types']
    data['forest_type_id'] = data['forest_type_name'].apply(lambda x: next((enum.name for enum in Forest_types if enum.value == x), None))
    
    # Find tree_type_id
    Tree_types = db_dims['Tree_types']
    data['tree_type_id'] = data['tree_type_name'].apply(lambda x: next((enum.name for enum in Tree_types if enum.value == x), None))

    # Find unit_ids
    Units = db_dims['Units']
    data['unit_id'] = data['unit_name'].apply(lambda x: next((enum.name for enum in Units if enum.value == x), None))
    
    # Find source_ids
    Sources = db_dims['Sources']
    data['source_id'] = data['source_name'].apply(lambda x: next((enum.name for enum in Sources if enum.value == x), None)) 
    
    db_data = data[['region_id', 'forest_type_id', 'tree_type_id', 'land_loss', 'unit_id','uncertainty', 
                    'creation_date','modified_date','validity_date', 'source_id']].copy()
        
    return db_data
