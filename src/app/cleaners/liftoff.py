from abc import ABC, abstractmethod
from typing import Union, List, Dict, Optional
import json


class ResponseCleanerFactory(ABC):

    def __init__(
            self,
            response: Union[List, Dict, None] = None,
            path_to_response: Optional[str] = None
    ) -> None:
        super().__init__()
        self.response = response if path_to_response is None else self.get_response_from_file(path_to_response)

    def get_response_from_file(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as file:
            self.response = json.load(file)

    @abstractmethod
    def clean(self) -> Union[List, Dict, None]:
        pass


class PostReportsCleaner(ResponseCleanerFactory):

    def clean(self) -> Union[List, Dict, None]:
        pass

    def get_id_from_response(self) -> str:
        return self.response.get("id")


class GetReportsIdDataCleaner(ResponseCleanerFactory):
    pass