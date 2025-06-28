from abc import ABC
from typing import Any, Union
from src.app import utils


class APITransformerFactory(ABC):

    def __init__(
            self,
            api_key: str,
            data: Union[dict, list]
    ) -> None:
        self.api_key = api_key
        self.data = data


class APIGetAppsTransformer(APITransformerFactory):

    def transform_to_one_level_of_nesting(self, data: list[dict[Any]] | None = None) -> None:
        data = self.data if data is None else data
        for row in self.data:
            row["optimization_event_id"] = row.get("optimization_event").get("id")
            row["optimization_event_name"] = row.get("optimization_event").get("name")
            del row["optimization_event"]
        return data
    

def main() -> None:
    local_connector = utils.LocalConnector()
    data = local_connector.extract_json_data("src/app/data/3aa24b5688-2025-06-26_21_29_38_348589.json")
    
    api_key = "3aa24b5688"
    transformer = APIGetAppsTransformer(api_key, data)
    trans_data = transformer.transform_to_one_level_of_nesting()

    path = local_connector.create_path(api_key, "app", "transformed")
    local_connector.save_json_data(path, trans_data)


if __name__ == "__main__":
    main()