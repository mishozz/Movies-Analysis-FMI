from dags.persistence.in_memory_db import ParquetDatabase
from dags.persistence.base_db_interface import DatabaseInterface

DEV_ENV = 'DEV'
PROD_ENV = 'PROD'


def get_database(env=DEV_ENV) -> DatabaseInterface:
    if env == DEV_ENV:
        return ParquetDatabase()
    elif env == PROD_ENV:
       raise NotImplementedError("Productio database is available yet")

    raise RuntimeError(f"Unknown environment '{env}'")
