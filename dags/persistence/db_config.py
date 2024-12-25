from dags.persistence.dataframe_repository import DataFrameReposotiry
from dags.persistence.persistence_interface import PersistenceInterface

DEV_ENV = 'DEV'
PROD_ENV = 'PROD'


def get_dataframe_repository(env=DEV_ENV) -> PersistenceInterface:
    if env == DEV_ENV:
        return DataFrameReposotiry()
    elif env == PROD_ENV:
       raise NotImplementedError("Productio database is available yet")

    raise RuntimeError(f"Unknown environment '{env}'")
