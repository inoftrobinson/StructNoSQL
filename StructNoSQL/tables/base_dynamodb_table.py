from typing import List
from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex


class BaseDynamoDBTable:
    def __setup__(
        self, table_name: str, region_name: str, primary_index: PrimaryIndex,
        billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
        global_secondary_indexes: List[GlobalSecondaryIndex] = None,
        auto_create_table: bool = True
    ):
        # super().__init__(data_model=data_model)
        self._table_name = table_name
        self._region_name = region_name
        self._dynamodb_client = DynamoDbCoreAdapter(
            table_name=table_name, region_name=region_name, billing_mode=billing_mode,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            create_table=auto_create_table
        )
        self._primary_index_name = primary_index.index_custom_name or primary_index.hash_key_name

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def region_name(self) -> str:
        return self._region_name

    @property
    def primary_index_name(self) -> str:
        return self._primary_index_name

    @property
    def dynamodb_client(self) -> DynamoDbCoreAdapter:
        return self._dynamodb_client
