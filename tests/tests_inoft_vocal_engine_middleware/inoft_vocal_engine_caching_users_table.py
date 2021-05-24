from typing import Optional, Any
from StructNoSQL import PrimaryIndex, GlobalSecondaryIndex, BasicTable, CachingTable
from StructNoSQL.tables.inoft_vocal_engine_caching_table import InoftVocalEngineCachingTable

TEST_ACCOUNT_ID = "5ae5938d-d4b5-41a7-ad33-40f3c1476211"
TEST_PROJECT_ID = "defcc77c-1d6d-46a4-8cbe-506d12b824b7"
TEST_ACCOUNT_EMAIL = "yay.com"
TEST_ACCOUNT_USERNAME = "Yay"

class InoftVocalEngineCachingUsersTable(InoftVocalEngineCachingTable):
    def __init__(self, data_model: Optional[Any] = None):
        # primary_index = PrimaryIndex(hash_key_name="accountId", hash_key_variable_python_type=str)
        super().__init__(table_id="structnosql-playground", region_name="eu-west-2", data_model=data_model)
