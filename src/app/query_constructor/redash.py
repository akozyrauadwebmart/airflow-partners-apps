from abc import ABC
from typing import Optional


class QueryConstructorFactory(ABC):
    pass


class RedashQueryConstructor(QueryConstructorFactory):

    def __init__(
            self,
            filters: Optional[list[dict[str]]] = None,
            filter_date: Optional[str] = None
    ) -> None:
        self.other_filters = filters
        self.date_filter = filter_date

    def create_filters_statment(self) -> str:
        filters_statment = "WHERE"
        date_filter = self.create_date_filter()
        other_filter = self.create_other_filters()
    
    def create_date_filter(self):
        pass

    def create_other_filters(self):
        pass

    def get_query(
            self,
            db_name: str,
            table_name: str,
            select_column: str
    ) -> str:
        query = f"""
            SELECT
                {select_column},
                SUM(sum_spend) AS sum_spend,
                SUM(sum_clicks) AS sum_clicks,
                SUM(sum_installs) AS sum_installs
            FROM {db_name}.{table_name}
            GROUP BY {select_column}
            ORDER BY {select_column}
        """
        return query