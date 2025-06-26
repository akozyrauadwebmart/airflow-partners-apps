from abc import ABC
from typing import Any
import json
from datetime import datetime


class APITransformerFactory(ABC):

    def __init__(
            self,
            api_key: str,
            local_path: str
    ) -> None:
        self.api_key = api_key
        self.data = self.get_data_from_local_path(local_path)

    def get_data_from_local_path(self, path: str) -> list[dict[Any]]:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)


class APIGetAppsTransformer(APITransformerFactory):

    def transform_to_one_level_of_nesting(self, data: list[dict[Any]] | None = None) -> None:
        data = self.data if data is None else data
        for row in self.data:
            row["optimization_event_id"] = row.get("optimization_event").get("id")
            row["optimization_event_name"] = row.get("optimization_event").get("name")
            del row["optimization_event"]
        return data