import boto3
from typing import Optional, Any
from StructNoSQL import DynamoDBBasicTable, PrimaryIndex, DynamoDBCachingTable


class ProdInoftVocalEngineTableBasicClient(DynamoDBBasicTable):
    def __init__(
            self, data_model: Optional[Any] = None,
            auto_create_table: bool = True,
            boto_session: Optional[boto3.Session] = None,
            auto_leading_key: Optional[str] = None
    ):
        primary_index = PrimaryIndex(hash_key_name='accountProjectTableKeyId', hash_key_variable_python_type=str)
        super().__init__(
            table_name="inoft-vocal-engine_projects-virtual-tables", region_name="eu-west-3",
            data_model=data_model, primary_index=primary_index,
            auto_create_table=auto_create_table,
            boto_session=boto_session,
            auto_leading_key=auto_leading_key
        )

class ProdInoftVocalEngineTableCachingClient(DynamoDBCachingTable):
    def __init__(
            self, data_model: Optional[Any] = None,
            auto_create_table: bool = True,
            boto_session: Optional[boto3.Session] = None,
            auto_leading_key: Optional[str] = None
    ):
        primary_index = PrimaryIndex(hash_key_name='accountProjectTableKeyId', hash_key_variable_python_type=str)
        super().__init__(
            table_name="inoft-vocal-engine_projects-virtual-tables", region_name="eu-west-3",
            data_model=data_model, primary_index=primary_index,
            auto_create_table=auto_create_table,
            boto_session=boto_session,
            auto_leading_key=auto_leading_key
        )
