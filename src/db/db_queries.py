import pandas as pd
import sqlite3
import logging

logging.basicConfig(level=logging.INFO)

def upload_data(data:pd.DataFrame, table:str) -> None:
    """
    Given a dataframe with all required data, and column names equal to the db table schema,
    upload / append new values to given database table.
    """
    db = sqlite3.connect('ecoact.db')

    try:
        data.to_sql(table, db, if_exists="replace", index=False)
        logging.info(f'Uploaded data to {table}.')
    except sqlite3.OperationalError as e:
        logging.error(e)
    
    finally:
        db.close()


def fetch_all_from_table(table:str) -> pd.DataFrame:
    """
    Given a db SQL table, return all results as a dataframe.
    """
    db = sqlite3.connect('ecoact.db')
    
    try:
        query = f"SELECT * FROM {table}"
        df = pd.read_sql_query(query, db)
        
        # For auto-incrementing primary keys
        df.reset_index(names='id', inplace=True)
        df['id'] +=1 # auto-incrementing ids start at 1, rather than 0 (in pd.DataFrames)
        
        logging.info(f'Fetch from {table} successful.')
    except sqlite3.OperationalError as e:
        logging.error(e)
    
    finally:
        db.close()
        
    return df
