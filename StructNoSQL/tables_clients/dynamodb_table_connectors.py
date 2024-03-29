import boto3
from typing import List, Optional
from StructNoSQL.tables_clients.backend.models import PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.tables_clients.backend.dynamodb_core import DynamoDbCoreAdapter


class DynamoDBTableConnectors:
    def __setup_connectors__(
        self, table_name: str, region_name: str, primary_index: PrimaryIndex,
        billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
        global_secondary_indexes: List[GlobalSecondaryIndex] = None,
        auto_create_table: bool = True,
        boto_session: Optional[boto3.Session] = None
    ):
        self._table_name = table_name
        self._region_name = region_name
        self._dynamodb_client = DynamoDbCoreAdapter(
            table_name=table_name, region_name=region_name, billing_mode=billing_mode,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            create_table=auto_create_table,
            boto_session=boto_session
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
