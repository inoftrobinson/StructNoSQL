from typing import Optional, Any
from StructNoSQL import PrimaryIndex, GlobalSecondaryIndex, DynamoDBCachingTable, InoftVocalEngineCachingTable

TEST_ACCOUNT_ID = "5ae5938d-d4b5-41a7-ad33-40f3c1476211"
TEST_PROJECT_ID = "defcc77c-1d6d-46a4-8cbe-506d12b824b7"
TEST_ACCOUNT_EMAIL = "yay.com"
TEST_ACCOUNT_USERNAME = "Yay"


class CachingUsersTable(DynamoDBCachingTable):
    def __init__(self, data_model):
        primary_index = PrimaryIndex(hash_key_name="accountId", hash_key_variable_python_type=str)
        globals_secondary_indexes = [
            GlobalSecondaryIndex(hash_key_name="username", hash_key_variable_python_type=str, projection_type="ALL"),
            GlobalSecondaryIndex(hash_key_name="email", hash_key_variable_python_type=str, projection_type="ALL"),
        ]
        super().__init__(
            table_name="structnosql-playground", region_name="eu-west-2", data_model=data_model,
            primary_index=primary_index, global_secondary_indexes=globals_secondary_indexes,
            auto_create_table=True
        )

class InoftVocalEngineUsersCachingTable(InoftVocalEngineCachingTable):
    def __init__(self, data_model):
        super().__init__(
            engine_account_id="b1fe5939-032b-462d-92e0-a942cd445096",
            engine_project_id="4ede8b70-46f6-4ae2-b09c-05a549194c8e",
            engine_api_key="a2bf5ff8-bbd3-4d01-b695-04138ee19b42",
            table_id="structnosql-playground", region_name="eu-west-2",
            data_model=data_model
        )
