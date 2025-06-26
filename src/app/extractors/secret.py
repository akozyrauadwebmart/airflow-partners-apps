from abc import ABC
from typing import Optional, Tuple, Any, Generator, Dict
import os

from clickhouse_connect.driver.client import Client
from dotenv import load_dotenv

from src.db.clickhouse.client_init import create_client
from src.db.clickhouse import models

load_dotenv()


class SecretExtractorFactory(ABC):
    
    def from_query_result_rows_to_tuple(
            self,
            query_result_rows: list[Tuple[Any]],
            index: Optional[int] = 0
    ) -> tuple[Any]:
        result_list = []
        for row in query_result_rows:
            result_list.append(row[index])
        return tuple(result_list)
    
    def from_named_results_to_tuple_of_dicts(self, named_results: Generator) -> tuple[Dict]:
        result_list = []
        for element in named_results:
            result_list.append(element)
        return tuple(result_list)


class LiftoffSecretExtractor(SecretExtractorFactory):

    def __init__(self, ch_client: Optional[Client] = None):
        self.ch_client = create_client() if ch_client is None else ch_client

    def get_tuple_of_api_keys(self) -> tuple[str]:
        query = f"""
            SELECT api_key
            FROM {models.LiftoffSecretAccountModel.FULL_NAME}
        """
        query_result = self.ch_client.query(query=query)
        return self.from_query_result_rows_to_tuple(query_result.result_rows)

    def get_api_secret_by_api_key(self, api_key: str) -> str:
        query = f"""
            SELECT api_secret
            FROM {models.LiftoffSecretAccountModel.FULL_NAME}
            WHERE api_key = '{api_key}'
        """
        query_result = self.ch_client.query(query=query)
        return query_result.first_item["api_secret"]
    
    def get_full_secret_data(self) -> tuple[dict[str]]:
        query = f"""
            SELECT api_key, api_secret
            FROM {models.LiftoffSecretAccountModel.FULL_NAME}
        """
        query_result = self.ch_client.query(query=query)
        return self.from_named_results_to_tuple_of_dicts(query_result.named_results())


def main() -> None:
    extractor = LiftoffSecretExtractor()
    res = extractor.get_full_secret_data()


if __name__ == "__main__":
    main()