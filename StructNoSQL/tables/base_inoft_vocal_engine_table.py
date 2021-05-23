import requests
from typing import List, Optional, Any
from StructNoSQL.tables.base_table import BaseTable


class BaseInoftVocalEngineTable(BaseTable):
    def __init__(self, table_id: str, region_name: str, data_model):
        super().__init__(data_model=data_model)
        self.table_id = table_id

    @staticmethod
    def _api_handler(payload: dict) -> Optional[Any]:
        response = requests.post(url='127.0.0.1:5000/api/database-client', json=payload)
        response_data: Optional[dict] = response.json()
        if response_data is not None and isinstance(response_data, dict):
            success: bool = response_data.get('success', False)
            if success is True:
                return response_data.get('data', None)
        return None
