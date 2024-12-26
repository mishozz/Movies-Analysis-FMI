from dags.persistence.dataframe_repository import DataFrameRepository
from dags.persistence.base_repository_interface import BaseRepositoryInterface

DEV_ENV = 'DEV'
PROD_ENV = 'PROD'

class RepositoryConfig:
    _repository = None
    
    @classmethod
    def get_repository_instance(cls):
        if cls._repository is None:
            cls._repository = cls._get_repository()
        return cls._repository

    @classmethod
    def _get_repository(cls, env=DEV_ENV) -> BaseRepositoryInterface:
        if env == DEV_ENV:
            return DataFrameRepository()
        elif env == PROD_ENV:
            raise NotImplementedError("Production environment not yet supported")
        else:
            raise RuntimeError(f"Unknown environment '{env}'")
