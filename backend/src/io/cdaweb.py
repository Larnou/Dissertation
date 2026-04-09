from dataclasses import dataclass
from typing import Any

from cdasws import CdasWs
from loguru import logger


@dataclass(frozen=True, slots=True)
class CDAweb:
    dataset_name: str
    client: CdasWs

    @staticmethod
    def default(dataset_name: str) -> "CDAweb":
        return CDAweb(dataset_name=dataset_name, client=CdasWs())


    def get_dataset(self, columns: list[str], since_time: str, until_time: str) -> Any:
        logger.info(
            f"CDAWeb download dataset={self.dataset_name} columns={columns} since={since_time} until={until_time}"
        )
        return self.client.get_data(self.dataset_name, columns, since_time, until_time)[1]


    def get_dataset_variables(self):
        variables = self.client.get_variables(self.dataset_name)
        return list(variables or [])


    def describe_variables(self) -> list[str]:
        return [
            f"{v.get('Name')}: {v.get('LongDescription')}"
            for v in self.get_dataset_variables()
        ]
