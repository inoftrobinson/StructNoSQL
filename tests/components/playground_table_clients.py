import boto3
from typing import Optional, Any
from StructNoSQL import DynamoDBBasicTable, PrimaryIndex, GlobalSecondaryIndex, DynamoDBCachingTable


TEST_ACCOUNT_ID = "5ae5938d-d4b5-41a7-ad33-40f3c1476211"
TEST_PROJECT_ID = "defcc77c-1d6d-46a4-8cbe-506d12b824b7"
TEST_ACCOUNT_EMAIL = "yay.com"
TEST_ACCOUNT_USERNAME = "Yay"


class PlaygroundDynamoDBBasicTable(DynamoDBBasicTable):
    def __init__(
            self, data_model: Optional[Any] = None,
            auto_create_table: bool = True,
            boto_session: Optional[boto3.Session] = None
    ):
        primary_index = PrimaryIndex(hash_key_name='accountId', hash_key_variable_python_type=str)
        globals_secondary_indexes = [
            GlobalSecondaryIndex(hash_key_name='username', hash_key_variable_python_type=str, projection_type='ALL'),
            GlobalSecondaryIndex(hash_key_name='email', hash_key_variable_python_type=str, projection_type='ALL'),
            GlobalSecondaryIndex(hash_key_name='type', hash_key_variable_python_type=str, projection_type='ALL'),
        ]
        super().__init__(
            table_name="structnosql-playground", region_name="eu-west-2", data_model=data_model,
            primary_index=primary_index, global_secondary_indexes=globals_secondary_indexes,
            auto_create_table=auto_create_table, boto_session=boto_session
        )

class PlaygroundDynamoDBCachingTable(DynamoDBCachingTable):
    def __init__(self, data_model):
        primary_index = PrimaryIndex(hash_key_name="accountId", hash_key_variable_python_type=str)
        globals_secondary_indexes = [
            GlobalSecondaryIndex(hash_key_name="username", hash_key_variable_python_type=str, projection_type='ALL'),
            GlobalSecondaryIndex(hash_key_name="email", hash_key_variable_python_type=str, projection_type='ALL'),
            GlobalSecondaryIndex(hash_key_name='type', hash_key_variable_python_type=str, projection_type='ALL'),
        ]
        super().__init__(
            table_name="structnosql-playground", region_name="eu-west-2", data_model=data_model,
            primary_index=primary_index, global_secondary_indexes=globals_secondary_indexes,
            auto_create_table=True
        )


PROD_ACCOUNT_ID = "b1fe5939-032b-462d-92e0-a942cd445096"
PROD_PROJECT_ID = "03731a00-5677-4f93-bb69-97fb29cb04e4"
ENGINE_API_KEY = "0a95e2a7-ba06-4f47-9864-70d275ab4b2f"
INOFT_VOCAL_ENGINE_PLAYGROUND_TABLE_KWARGS = {
    'engine_account_id': PROD_ACCOUNT_ID,
    'engine_project_id': PROD_PROJECT_ID,
    'engine_api_key': ENGINE_API_KEY,
    'table_id': "structnosql-playground",
    'region_name': "eu-west-2",
}
