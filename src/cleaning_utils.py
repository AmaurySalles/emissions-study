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