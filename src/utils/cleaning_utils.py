import pandas as pd
import locale
from datetime import datetime

def clean_FR_dates(data:pd.Series, input_format:str) -> pd.Series:
    # set the locale to French
    locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')
    
    # Ensure months have accents
    data = data.replace({'Fevrier':'Février', 'Aout':'Août','Decembre':'Décembre'}, regex=True)
    
    # Convert to FR datetime
    data = data.apply(lambda x: datetime.strptime(x, input_format))

    return pd.to_datetime(data)

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
        "Midi-Pyrenées":"Occitanie",
        "Nord-Pas-de-Calais": "Hauts de France",
        "Picardie":"Hauts de France",
        "Basse-Normandie": "Normandie",
        "Haute-Normandie": "Normandie",
    }
    data.replace(region_name_map, regex=True, inplace=True)
    
    return data.str.strip()