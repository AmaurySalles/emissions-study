import pandas as pd
from pathlib import Path
import logging

from src.db.db_setup import create_db, init_db_dim_data, retrieve_db_dim_data
from src.dataset_func.fr_regional_heating_emissions_data import import_fr_regional_heating_emissions_data
from src.dataset_func.fr_electricity_emissions_data import import_fr_electricity_emissions_data
from src.dataset_func.world_electricity_emissions_data import import_world_electricity_emissions_data
from src.dataset_func.fr_forestry_area_data import import_FR_forestry_area_data

logging.basicConfig(level=logging.INFO)

def main() -> None:
    
    data_file_path = Path.cwd() / "data" / "donnees_candidats_dev_python.csv"
    full_dataset = pd.read_csv(data_file_path, encoding='latin-1')
    
    db_dims = retrieve_db_dim_data()
    
    datasets = [
        import_fr_regional_heating_emissions_data,
        import_fr_electricity_emissions_data,
        import_world_electricity_emissions_data,
        import_FR_forestry_area_data
        # TODO: goods_emissions_data
        # TODO: transport_emissions_data
        # TODO: fuel_emissions_data
        # TODO: eletricity_production_data
        # TODO: waste_treatement_data
        # TODO: land_use_data
        # TODO: ...
    ]
    
    for dataset_func in datasets:
        try:
            dataset_func(full_dataset, db_dims)
        except Exception as e:
            logging.error(e)

if __name__=="__main__":
    create_db()
    init_db_dim_data()
    main()