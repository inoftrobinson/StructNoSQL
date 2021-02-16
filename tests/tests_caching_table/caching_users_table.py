from typing import Optional, Any
from StructNoSQL import PrimaryIndex, GlobalSecondaryIndex, BasicTable, CachingTable

TEST_ACCOUNT_ID = "5ae5938d-d4b5-41a7-ad33-40f3c1476211"
TEST_PROJECT_ID = "defcc77c-1d6d-46a4-8cbe-506d12b824b7"
TEST_ACCOUNT_EMAIL = "yay.com"
TEST_ACCOUNT_USERNAME = "Yay"

class CachingUsersTable(CachingTable):
    def __init__(self, data_model: Optional[Any] = None):
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
