from typing import Optional, List, Any

from StructNoSQL.middlewares.dynamodb.backend.dynamodb_core import Response
from StructNoSQL.models import DatabasePathElement


class Query:
    def __init__(
            self, table, variable_validator: Any, key_value: str, index_name: Optional[str] = None,
            target_database_path: Optional[List[DatabasePathElement]] = None,
    ):
        from StructNoSQL.tables import BaseTable
        from StructNoSQL.fields import BaseField
        self._table: BaseTable = table
        self._variable_validator: BaseField = variable_validator

        self.target_database_path = target_database_path
        self.key_value = key_value
        self.index_name = index_name
        self._query_limit = None

    def query_limit(self, limit: int):
        self._query_limit = limit

    def where(self) -> str:
        pass

    def first_item(self) -> Optional[dict]:
        pass

    def first_value(self) -> Optional[dict]:
        if self.target_database_path is not None:
            response = self._table.dynamodb_client.get_value_in_path_target(
                index_name=self.index_name, key_value=self.key_value,
                field_path_elements=self.target_database_path
            )
            self._variable_validator.populate(value=response)
            self._variable_validator.validate_data()
            return self._variable_validator.value
        else:
            # todo: improve that, and return the value not the item itself
            response = self._table.dynamodb_client.query_single_item_by_key(
                index_name=self.index_name, key_value=self.key_value,
            )

    def set_update(self, value: Any) -> Optional[Response]:
        if self.target_database_path is not None:
            self._variable_validator.populate(value=value)
            validated_data, valid = self._variable_validator.validate_data()
            if valid is True:
                response = self._table.dynamodb_client.set_update_data_element_to_map_with_default_initilization(
                    index_name=self.index_name, key_value=self.key_value,
                    field_path_elements=self.target_database_path, value=validated_data
                )
                return response
        return None
