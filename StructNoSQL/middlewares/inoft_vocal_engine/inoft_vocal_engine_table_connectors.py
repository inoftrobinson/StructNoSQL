from json import JSONDecodeError

import requests
from typing import List, Optional, Any, Dict

from StructNoSQL.models import FieldPathSetter, DatabasePathElement


class InoftVocalEngineTableConnectors:
    def __setup_connectors__(self, engine_account_id: str, engine_project_id: str, engine_api_key: str, table_id: str, region_name: str):
        self.engine_account_id = engine_account_id
        self.engine_project_id = engine_project_id
        self.engine_api_key = engine_api_key
        self.table_id = table_id
        self.region_name = region_name
        self.database_client_api_endpoint_url = f'http://127.0.0.1:5000/api/v1/{self.engine_account_id}/{self.engine_project_id}/database-client'

    def _base_api_handler(self, payload: dict) -> Optional[dict]:
        response = requests.post(
            url=self.database_client_api_endpoint_url,
            json={**payload, 'accessToken': self.engine_api_key}
        )
        if not response.ok:
            print("Response not ok")
            return None
        else:
            try:
                response_data: Optional[dict] = response.json()
                return response_data if response_data is not None and isinstance(response_data, dict) else None
            except JSONDecodeError as e:
                print(f"JSON decoding error : {e} : {response.text}")
                return None

    def _data_api_handler(self, payload: dict) -> Optional[Any]:
        response_data: Optional[dict] = self._base_api_handler(payload=payload)
        if response_data is not None:
            success: bool = response_data.get('success', False)
            if success is True:
                return response_data.get('data', None)
            """exception: Optional[str] = response_data.get('exception', None)
            if exception is not None:
                raise Exception(exception)"""
        return None

    def _success_api_handler(self, payload: dict) -> bool:
        response_data: Optional[dict] = self._base_api_handler(payload=payload)
        if response_data is not None:
            success: bool = response_data.get('success', False)
            return success
        return False

    def _put_record_request(self, record_item_data: dict) -> bool:
        return self._success_api_handler(payload={
            'operationType': 'putRecord',
            'recordItemData': record_item_data,
        })

    def _delete_record_request(self, indexes_keys_selectors: Dict[str, Any]) -> bool:
        return self._success_api_handler(payload={
            'operationType': 'deleteRecord',
            'indexesKeysSelectors': indexes_keys_selectors,
        })

    def _remove_record_request(self, indexes_keys_selectors: Dict[str, Any]) -> Optional[dict]:
        return self._data_api_handler(payload={
            'operationType': 'removeRecord',
            'indexesKeysSelectors': indexes_keys_selectors,
        })

    def _query_items_by_key(
            self, key_value: str, field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]],
            has_multiple_fields_path: bool, query_limit: int, filter_expression: Optional[Any] = None, **additional_kwargs
    ):
        serialized_fields_path_elements: List[dict] or Dict[str, dict] = [
            item.serialize() for item in field_path_elements
        ] if has_multiple_fields_path is not True else {
            key: [item.serialize() for item in path_elements]
            for key, path_elements in field_path_elements.items()
        }
        return self._data_api_handler(payload={
            'operationType': 'queryItemsByKey',
            'keyValue': key_value,
            'hasMultipleFieldsPath': has_multiple_fields_path,
            'fieldPathElements': serialized_fields_path_elements,
            'queryLimit': query_limit,
            'filterExpression': filter_expression,
            **additional_kwargs
        })

    def _set_update_multiple_data_elements_to_map(self, key_value: Any, setters: List[FieldPathSetter]) -> bool:
        serialized_setters: List[dict] = [item.serialize() for item in setters]
        return self._success_api_handler(payload={
            'operationType': 'setUpdateMultipleDataElementsToMap',
            'keyValue': key_value,
            'setters': serialized_setters
        })

    def _remove_data_elements_from_map(self, key_value: str, fields_path_elements: List[List[DatabasePathElement]]) -> bool:
        serialized_fields_path_elements: List[List[dict]] = [
            [item.serialize() for item in path_elements]
            for path_elements in fields_path_elements
        ]
        return self._data_api_handler(payload={
            'operationType': 'removeDataElementsFromMap',
            'keyValue': key_value,
            'fieldsPathElements': serialized_fields_path_elements
        })

    def _delete_data_elements_from_map(self, key_value: str, fields_path_elements: List[List[DatabasePathElement]]) -> bool:
        serialized_fields_path_elements: List[List[dict]] = [
            [item.serialize() for item in path_elements]
            for path_elements in fields_path_elements
        ]
        return self._success_api_handler(payload={
            'operationType': 'deleteDataElementsFromMap',
            'keyValue': key_value,
            'fieldsPathElements': serialized_fields_path_elements
        })

    def _get_single_value_in_path_target(self, key_value: str, field_path_elements: List[DatabasePathElement]) -> Optional[Any]:
        serialized_field_path_elements: List[dict] = [item.serialize() for item in field_path_elements]
        return self._data_api_handler(payload={
            'operationType': 'getSingleValueInPathTarget',
            'keyValue': key_value,
            'fieldPathElements': serialized_field_path_elements
        })

    def _get_values_in_multiple_path_target(self, key_value: str, fields_path_elements: Dict[str, List[DatabasePathElement]]) -> Optional[Any]:
        serialized_fields_path_elements: Dict[str, List[dict]] = {
            field_key: [item.serialize() for item in field_path_elements_items]
            for field_key, field_path_elements_items in fields_path_elements.items()
        }
        return self._data_api_handler(payload={
            'operationType': 'getValuesInMultiplePathTarget',
            'keyValue': key_value,
            'fieldsPathElements': serialized_fields_path_elements
        })

    def _get_or_query_single_item(self, key_value: str, fields_path_elements: List[List[DatabasePathElement]]) -> Optional[Any]:
        serialized_fields_path_elements: List[List[dict]] = [
            [item.serialize() for item in items_container]
            for items_container in fields_path_elements
        ]
        return self._data_api_handler(payload={
            'operationType': 'getOrQuerySingleItem',
            'keyValue': key_value,
            'fieldsPathElements': serialized_fields_path_elements
        })

