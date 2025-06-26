from typing import Literal, Union
from datetime import datetime
import json


class LocalPathCreator:

    @staticmethod
    def create_path(
            api_key: str,
            entity: Literal["app", "report", "campaign", "creative"],
            data_stage: Literal["row", "trans", "cleaned", "enriched"]
    ) -> str:
        now = str(datetime.now()).replace(" ", "_").replace(":", "_").replace(".", "_")
        return f"stc/app/data/{entity}_{data_stage}_data_{api_key}_{now}"
    

def extract_data_from_local_json(path) -> Union[list, dict]:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)