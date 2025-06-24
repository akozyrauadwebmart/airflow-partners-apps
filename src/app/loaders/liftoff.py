from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Union
import json

from clickhouse_connect.driver.client import Client
import pandas

from src.db.clickhouse import models


class LoaderFactory(ABC):

    def __init__(
            self,
            data: Optional[Union[List, Dict]] = None,
            path_to_data: Optional[str] = None
    ) -> None:
        self.data = self.get_data_from_local_storage(path_to_data) if data is None else data

    def get_data_from_local_storage(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data
    

class GetReportsIdDataStagingLoader(LoaderFactory):

    def __init__(
            self,
            data: Optional[Union[List, Dict]] = None,
            path_to_data: Optional[str] = None,
            model: models.ModelFactory = models.LiftoffStagingReportModel
    ) -> None:
        super().__init__(data, path_to_data)
        self.model = model

    def load_data_to_clickhouse(
            self,
            ch_client: Client
    ) -> None:
        df = pandas.DataFrame(self.data)
        ch_client.insert_df(
            database=self.model.DB_NAME,
            table=self.model.TABLE_NAME,
            df=df
        )
        