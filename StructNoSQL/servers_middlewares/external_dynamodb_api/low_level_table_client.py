import boto3
from typing import List, Optional
from StructNoSQL import DynamoDBTableConnectors, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.clients_middlewares.dynamodb.backend.dynamodb_core import DynamoDbCoreAdapter


class DynamoDBLowLevelTableClient(DynamoDBTableConnectors):
    def __init__(
            self, table_name: str, region_name: str, primary_index: PrimaryIndex,
            billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
            global_secondary_indexes: List[GlobalSecondaryIndex] = None,
            auto_create_table: bool = True,
            boto_session: Optional[boto3.Session] = None
    ):
        self.__setup_connectors__(
            table_name=table_name, region_name=region_name,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            billing_mode=billing_mode, auto_create_table=auto_create_table,
            boto_session=boto_session
        )
