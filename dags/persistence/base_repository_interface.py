from abc import ABC, abstractmethod
from typing import Any

class BaseRepositoryInterface(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def save_dataframe(self, name: str, df: Any):
        pass

    @abstractmethod
    def load_dataframe(self, name: str) -> Any:
        pass

    @abstractmethod
    def save_figure(self, name: str, fig: Any):
        pass

    @abstractmethod
    def load_figure(self, name: str) -> Any:
        pass

    @abstractmethod
    def save_data(self, name: str, data: Any):
        pass

    @abstractmethod
    def load_data(self, name: str) -> Any:
        pass