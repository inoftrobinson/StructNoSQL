import unittest

from StructNoSQL import BaseField, FieldSetter
from tests.tests_caching_table.test_query_operations.table_model import QueryOperationsBaseTableModel
from tests.users_table import UsersTable


class DynamoDBTableModel(QueryOperationsBaseTableModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)


class TestDynamoDBCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = UsersTable(data_model=DynamoDBTableModel)

    def test_set_get_fields_with_primary_index(self):
        from tests.tests_caching_table.test_query_operations.cases_shared import test_set_get_fields_with_primary_index
        test_set_get_fields_with_primary_index(self, users_table=self.users_table, primary_key_name='accountId', is_caching=False)

    def test_set_get_fields_with_overriding_names(self):
        from tests.tests_caching_table.test_query_operations.cases_shared import test_set_get_fields_with_overriding_names
        test_set_get_fields_with_overriding_names(self, users_table=self.users_table, primary_key_name='accountId', is_caching=False)

    def test_set_get_fields_with_multi_selectors(self):
        from tests.tests_caching_table.test_query_operations.cases_shared import test_set_get_fields_with_multi_selectors
        test_set_get_fields_with_multi_selectors(self, users_table=self.users_table, primary_key_name='accountId', is_caching=False)

    def test_set_get_fields_with_secondary_index(self):
        from tests.tests_caching_table.test_query_operations.cases_dynamodb import test_set_get_fields_with_secondary_index
        test_set_get_fields_with_secondary_index(self, users_table=self.users_table, is_caching=False)


if __name__ == '__main__':
    unittest.main()
