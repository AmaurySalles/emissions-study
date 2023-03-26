import pandas as pd
from pathlib import Path

from src.db.db_setup import create_db
from src.db.db_init_dims import init_db_dim_data
from src.dataset_func.fr_regional_heat_data import fr_regional_heating_emissions_data
from src.dataset_func.fr_electricity_data import fr_electricity_mix_data

def main() -> None:
    
    data_file_path = Path.cwd() / "data" / "donnees_candidats_dev_python.csv"
    full_dataset = pd.read_csv(data_file_path, encoding='latin-1')
    
    datasets = [
        fr_regional_heating_emissions_data,
        # TODO: fr_electricity_mix_data,
        # TODO: world_electricity_mix_data,
        # TODO: good_emissions_data
        # TODO: transport_emissions_data
        # TODO: fuel_emissions_data
        # TODO: ...
    ]
    
    for dataset_func in datasets:
        dataset_func(full_dataset)

if __name__=="__main__":
    create_db()
    init_db_dim_data()
    main()