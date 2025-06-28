import os
from typing import Literal, Union
from datetime import datetime
import json


class LocalConnector:

    def create_path(
            self,
            api_key: str,
            entity: Literal["app", "report", "campaign", "creative"],
            data_stage: Literal["row", "trans", "cleaned", "enriched"]
    ) -> str:
        now = str(datetime.now()).replace(" ", "_").replace(":", "_").replace(".", "_").replace("-", "_")
        return f"src/app/data/{entity}_{data_stage}_data_{api_key}_{now}.json"

    def extract_json_data(self, path) -> Union[list, dict]:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    
    def save_json_data(self, path: str, data: Union[dict, list]) -> None:
        with open(path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False, default=str)
        print(f"Data was saved in: {path}")

    def remove_file(self, path) -> None:
        os.remove(path)