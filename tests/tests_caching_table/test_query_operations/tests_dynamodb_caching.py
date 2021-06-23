import random
import unittest

from StructNoSQL import BaseField, FieldSetter
from tests.tests_caching_table.caching_users_table import CachingUsersTable, TEST_ACCOUNT_USERNAME, TEST_ACCOUNT_ID
from tests.tests_caching_table.test_query_operations.table_model import QueryOperationsBaseTableModel


class DynamoDBTableModel(QueryOperationsBaseTableModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)


class TestDynamoDBCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = CachingUsersTable(data_model=DynamoDBTableModel)
        self.users_table.debug = True

    def test_set_get_fields_with_primary_index(self):
        from tests.tests_caching_table.test_query_operations.cases_shared import test_set_get_fields_with_primary_index
        test_set_get_fields_with_primary_index(self, users_table=self.users_table, primary_key_name='accountId', is_caching=True)

    def test_set_get_fields_with_overriding_names(self):
        from tests.tests_caching_table.test_query_operations.cases_shared import test_set_get_fields_with_overriding_names
        test_set_get_fields_with_overriding_names(self, users_table=self.users_table, primary_key_name='accountId', is_caching=True)

    def test_set_get_fields_with_secondary_index(self):
        from tests.tests_caching_table.test_query_operations.cases_dynamodb import test_set_get_fields_with_secondary_index
        test_set_get_fields_with_secondary_index(self, users_table=self.users_table, is_caching=True)


if __name__ == '__main__':
    unittest.main()
