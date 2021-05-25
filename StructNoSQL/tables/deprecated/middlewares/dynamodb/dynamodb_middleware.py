from typing import Optional, List, Dict, Any

from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.dynamodb.models import DatabasePathElement
from StructNoSQL.tables.deprecated.middlewares.base_middleware import BaseMiddleware


class DynamoDBMiddleWare(BaseMiddleware):
    def __init__(
            self, table_name: str, region_name: str, primary_index: PrimaryIndex,
            global_secondary_indexes: List[GlobalSecondaryIndex] = None,
            billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
            auto_create_table: bool = True
    ):
        super().__init__(primary_index=primary_index)
        self._dynamodb_client = DynamoDbCoreAdapter(
            table_name=table_name, region_name=region_name, billing_mode=billing_mode,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            create_table=auto_create_table
        )

    @property
    def dynamodb_client(self) -> DynamoDbCoreAdapter:
        return self._dynamodb_client

    def get_field(
            self, has_multiple_fields_path: bool, field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]],
            key_value: str, index_name: Optional[str] = None
    ) -> Any:
        if has_multiple_fields_path is not True:
            field_path_elements: List[DatabasePathElement]
            response_data = self.dynamodb_client.get_value_in_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, field_path_elements=field_path_elements
            )
            return response_data
        else:
            field_path_elements: Dict[str, List[DatabasePathElement]]
            response_data = self.dynamodb_client.get_values_in_multiple_path_target(
                index_name=index_name or self.primary_index_name,
                key_value=key_value, fields_paths_elements=field_path_elements
            )
            return response_data
