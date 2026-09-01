from dataclasses import dataclass
from typing import Any

from cdasws import CdasWs

from backend.src.log import get_logger

logger = get_logger()


@dataclass(frozen=True, slots=True)
class CDAweb:
    """Тонкая обертка над `cdasws.CdasWs` для загрузки данных CDAWeb."""

    dataset_name: str
    client: CdasWs

    @staticmethod
    def default(dataset_name: str) -> "CDAweb":
        """Создает клиент CDAWeb с дефолтными настройками."""
        return CDAweb(dataset_name=dataset_name, client=CdasWs())


    def get_dataset(self, columns: list[str], since_time: str, until_time: str) -> Any:
        """Загружает выбранные переменные датасета за указанный интервал."""
        logger.info(f"CDAWeb download dataset={self.dataset_name} columns={columns} since={since_time} until={until_time}")
        response = self.client.get_data(self.dataset_name, columns, since_time, until_time)
        return response[1]


    def get_dataset_variables(self) -> list[dict[str, Any]]:
        """Возвращает метаданные переменных датасета."""
        variables = self.client.get_variables(self.dataset_name)
        return list(variables or [])


    def describe_variables(self) -> list[str]:
        """Возвращает краткие описания переменных датасета."""
        return [
            f"{v.get('Name')}: {v.get('LongDescription')}"
            for v in self.get_dataset_variables()
        ]
