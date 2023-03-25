import pandas as pd
import numpy as np
from pathlib import Path


from src.db_setup import create_db
from src.cleaning_utils import clean_FR_dates


def main() -> None:
    
    # Read excel file
    data_file_path = Path.cwd() / "data" / "donnees_candidats_dev_python.csv"
    data = pd.read_csv(data_file_path, encoding='latin-1')
    
    # Split main categories
    data[['Cat1', 'Cat2', 'Cat3', 'Cat4', 'Cat5', 'Cat6']] = data["Code de la catégorie"].str.split(" > ", expand=True)
    
    # Regional Heat Map
    regional_heat_data = data[data['Cat1']=="Réseaux de chaleur / froid"].copy()
    regional_heat_data = clean_regional_heat_data(regional_heat_data)

    ## Upload to DB
    regional_heat_data.to_csv('./data/regional_heat_data_clean.csv', encoding='latin-1')


def clean_regional_heat_data(data:pd.DataFrame) -> pd.DataFrame:
    clean_df = pd.DataFrame()    
    
    # Country
    clean_df['country'] = data['Localisation géographique'].apply(lambda x: x.split()[0])
    
    # Region
    data.loc[data["Sous-localisation géographique français"].isna(), "Sous-localisation géographique français"] = 'Corse, Corte' # From data-explore
    clean_df['region'] = data['Sous-localisation géographique français'].apply(lambda x: x.split(',')[0]) # Take only region-name
    
    # Split into dept, city and addresses
    clean_df[['dept_id', 'city', 'address', 'address2']] = data['Nom attribut français'].str.split(', ', expand=True)
    clean_df['address'] = clean_df['address'].str.cat(clean_df['address2'], sep=' ', na_rep='') # Concat & remove as only 2 val
    clean_df.drop(columns={'address2'}, inplace=True) 
    
    # Heat_cycle (shorten value)
    clean_df['heat_cycle'] = data['Nom base français'].apply(lambda x: 'heat' if 'chaleur' in x else 'froid')
    
    # Format Dates
    clean_df['creation_date'] = clean_FR_dates(data['Date de création'], input_format='%B %Y')
    clean_df['modified_date'] = clean_FR_dates(data['Date de modification'], input_format='%B %Y')
    clean_df['validity_date'] = pd.to_datetime(data['Période de validité'].apply(lambda x: x.replace('Année ', '')), format='%Y')
    
    # Archive status
    # TODO: Create general 'status' function to check for other possible 'Status' values - only two values in this subset: ['Archivé' 'Valide générique']
    clean_df['archived'] = data["Statut de l'élément"].apply(lambda x: True if x=='Archivé' else False) 
    
    # Emmission values
    clean_df['emissions'] = data['Total poste non décomposé']
    dept_mean_emissions = clean_df[clean_df['emissions'] != 0].groupby('dept_id')['emissions'].mean() # TODO: Also filter by validity_date
    clean_df['emissions'] = clean_df.apply(lambda x: dept_mean_emissions[x['dept_id']] if x['emissions'] == 0 else x['emissions'], axis=1)
    
    # Uncertainty values
    clean_df['uncertainty'] = data['Incertitude']
    
    # Units
    clean_df['unit'] = data['Unité français']
    
    # Source
    clean_df['source'] = data['Source']
    
    return clean_df


if __name__=="__main__":
    create_db()
    main()