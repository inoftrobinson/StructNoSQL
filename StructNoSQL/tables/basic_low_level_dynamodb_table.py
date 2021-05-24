from typing import List, Optional, Dict, Any

from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.dynamodb.models import DatabasePathElement
from StructNoSQL.tables.base_dynamodb_table import BaseDynamoDBTable


class BasicLowLevelDynamoDBTable(BaseDynamoDBTable):
    def __init__(
        self, table_name: str, region_name: str, primary_index: PrimaryIndex,
        billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
        global_secondary_indexes: List[GlobalSecondaryIndex] = None,
        auto_create_table: bool = True
    ):
        super().__setup__(
            table_name=table_name, region_name=region_name,
            primary_index=primary_index, billing_mode=billing_mode,
            global_secondary_indexes=global_secondary_indexes, auto_create_table=auto_create_table
        )

    def get_field(
            self, has_multiple_fields_path: bool,
            field_path_elements: List[DatabasePathElement] or Dict[str, List[DatabasePathElement]],
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