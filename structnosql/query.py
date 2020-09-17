from typing import Optional, List, Dict, Any


class Query:
    def __init__(
            self, table, variable_validator: Any, key_name: str, key_value: str, index_name: Optional[str] = None,
            target_database_path: Optional[Dict[str, type]] = None,
    ):
        from StructNoSQL.table import BaseTable
        from StructNoSQL.fields import BaseItem
        self._table: BaseTable = table
        self._variable_validator: BaseItem = variable_validator

        self.target_database_path = target_database_path
        self.key_name = key_name
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
                key_name=self.key_name, key_value=self.key_value,
                target_path_elements=self.target_database_path
            )
            print(self._variable_validator.populate(value=response))
            print(response)
            return response
        else:
            # todo: improve that, and return the value not the item itself
            response = self._table.dynamodb_client.query_single_item_by_key(
                key_name=self.key_name, key_value=self.key_value, index_name=self.index_name,
            )

