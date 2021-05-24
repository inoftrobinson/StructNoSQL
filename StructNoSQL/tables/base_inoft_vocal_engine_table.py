import json
from json import JSONDecodeError

import requests
from typing import List, Optional, Any
from StructNoSQL.tables.base_table import BaseTable


class InoftVocalEngineTableConnectors:
    def __setup_connectors__(self, table_id: str, region_name: str):
        self.table_id = table_id
        self.region_name = region_name

    @staticmethod
    def _api_handler(payload: dict) -> Optional[Any]:
        response = requests.post(url='http://127.0.0.1:5000/api/v1/database-client', json=payload)
        if not response.ok:
            print("Response not ok")
            return None
        else:
            try:
                response_data: Optional[dict] = response.json()
                if response_data is not None and isinstance(response_data, dict):
                    success: bool = response_data.get('success', False)
                    if success is True:
                        return response_data.get('data', None)
                return None
            except JSONDecodeError as e:
                print(f"JSON decoding error : {e} : {response.text}")
                return None
