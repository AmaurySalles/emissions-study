import pandas as pd

from src.utils.cleaning_utils import clean_FR_dates
from src.db.db_queries import fetch_all_from_table, upload_data

DB_TABLE = 'F_world_electricity_emissions'

def import_world_electricity_emissions_data(full_dataset:pd.DataFrame) -> None:
    data = retrieve_world_electricity_emissions_data(full_dataset)
    data = clean_world_electricity_emissions_data(data)
    data = prep_world_electricity_emissions_data(data)
    upload_data(data, DB_TABLE)

def retrieve_world_electricity_emissions_data(data:pd.DataFrame) -> pd.DataFrame:
    # Split main categories
    data[['Cat1', 'Cat2', 'Cat3', 'Cat4', 'Cat5', 'Cat6']] = data["Code de la catégorie"].str.split(" > ", expand=True)
    
    # Find world electricity emissions data
    electricity_data = data[data['Cat1']=="Electricité"]
    elec_mix_data = electricity_data[electricity_data['Cat2']=="Mix réseau électrique"]
    world_elec_mix_data = elec_mix_data[elec_mix_data['Cat3']=="Autres pays du monde"].copy()
    
    return world_elec_mix_data

def clean_world_electricity_emissions_data(data:pd.DataFrame) -> pd.DataFrame:
    clean_df = pd.DataFrame()    
    
    clean_df['fr_country_name'] = data['Sous-localisation géographique français']
    clean_df['fr_country_name'].replace({"Birmanie":"Myanmar",
                                      "Bélarus":"Biélorussie",
                                      "Congo": "République du Congo",
                                      "Corée du nord": "Corée du Nord",
                                      "Corée du sud": "Corée du Sud",
                                      "Costa rica": "Costa Rica",
                                      "Côte d'Ivoire": "Côte dIvoire", # DB does not take special charaters
                                      "Dominicaine. République":"République Dominicaine",
                                      "Iraq":"Irak",
                                      "Macédonie":"Macédoine",
                                      "Rép. Dém. Du Congo":"République Démocratique du Congo"},
                                    regex=True, inplace=True)
    clean_df[clean_df['fr_country_name']!='Union européenne à 27'] # Remove EU-27 (no corresponding country in DB & can be reconstructed)
    
    clean_df['validity_date'] = data['Période de validité'].apply(lambda x: x.replace('déc.-', 'Décembre 20'))
    clean_df['validity_date'] = clean_df['validity_date'].apply(lambda x: x.replace('déc-', 'Décembre 20'))                                                 
    clean_df['validity_date'] = pd.to_datetime(clean_df['validity_date'], format='%B %Y')
    
    clean_df['source_type_name'] = data['Type poste'].fillna('Total')
    clean_df['unit_name'] = data['Unité français']
    clean_df['emissions'] = data['Total poste non décomposé']
    clean_df['source_name'] = data['Source']
    clean_df['uncertainty'] = data['Incertitude']
    clean_df['creation_date'] = clean_FR_dates(data['Date de modification'], input_format='%B %Y')
    clean_df['modified_date'] = clean_FR_dates(data['Date de modification'], input_format='%B %Y')
    
    return clean_df

def prep_world_electricity_emissions_data(data:pd.DataFrame) -> pd.DataFrame:
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