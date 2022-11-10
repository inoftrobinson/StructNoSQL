import boto3
from typing import Optional, Any, List
from pydantic import ValidationError
from StructNoSQL import DynamoDBTableConnectors, GlobalSecondaryIndex, PrimaryIndex
from StructNoSQL.clients_middlewares.dynamodb.backend.dynamodb_core import DynamoDbCoreAdapter
from StructNoSQL.servers_middlewares.external_dynamodb_api.low_level_table_client import DynamoDBLowLevelTableClient
from StructNoSQL.servers_middlewares.external_dynamodb_api.parser import StructNoSQLParser


class ExternalDynamoDBApiExecutor(DynamoDBTableConnectors):
    def __init__(
            self, table_name: str, region_name: str, primary_index: PrimaryIndex,
            billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
            global_secondary_indexes: List[GlobalSecondaryIndex] = None,
            auto_create_table: bool = True,
            boto_session: Optional[boto3.Session] = None
    ):
        self.table = DynamoDBLowLevelTableClient(
            table_name=table_name, region_name=region_name,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            billing_mode=billing_mode, auto_create_table=auto_create_table,
            boto_session=boto_session
        )

    def execute(self, data: dict) -> dict:
        handler: Optional[Any] = StructNoSQLParser.parse(data=data)
        if handler is None:
            return {'success': False, 'errorKey': 'operationNotSupported'}

        try:
            success, data, kwargs = handler(self.table, data)
            return {'success': success, 'data': data, **(kwargs or {})}
        except ValidationError as e:
            return {'success': False, 'errorKey': 'validationError', 'exception': str(e)}
