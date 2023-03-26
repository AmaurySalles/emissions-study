import pandas as pd
from pathlib import Path
import logging

from src.db.db_setup import create_db, init_db_dim_data
from src.dataset_func.fr_regional_heating_emissions_data import import_fr_regional_heating_emissions_data
from src.dataset_func.fr_electricity_mix_data import import_fr_electricity_mix_data

logging.basicConfig(level=logging.INFO)

def main() -> None:
    
    data_file_path = Path.cwd() / "data" / "donnees_candidats_dev_python.csv"
    full_dataset = pd.read_csv(data_file_path, encoding='latin-1')
    
    datasets = [
        import_fr_regional_heating_emissions_data,
        import_fr_electricity_mix_data,
        # TODO: world_electricity_mix_data,
        # TODO: goods_emissions_data
        # TODO: transport_emissions_data
        # TODO: fuel_emissions_data
        # TODO: ...
    ]
    
    for dataset_func in datasets:
        try:
            dataset_func(full_dataset)
        except Exception as e:
            logging.error(e)

if __name__=="__main__":
    create_db()
    init_db_dim_data()
    main()