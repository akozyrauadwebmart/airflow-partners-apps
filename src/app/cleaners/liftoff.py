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


class PostReportsCleaner(ResponseCleanerFactory):

    def get_id_from_response(self) -> str:
        return self.response.get("id")


class GetReportsIdDataCleaner(ResponseCleanerFactory):

    def check_state(self) -> bool:
        state = self.get_status()
        return self.is_ready_to_download(state)
    
    def get_status(self) -> str:
        return self.response.get("state")
    
    def is_ready_to_download(self, state: str) -> bool:
        return state == "completed"