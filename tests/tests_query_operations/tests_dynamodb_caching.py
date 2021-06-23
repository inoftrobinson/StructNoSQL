import unittest

from tests.components.playground_table_clients import PlaygroundDynamoDBCachingTable
from tests.tests_query_operations.table_models import DynamoDBTableModel


class TestDynamoDBCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBCachingTable(data_model=DynamoDBTableModel)
        self.users_table.debug = True

        self.DYNAMODB_CASE_KWARGS = {'self': self, 'users_table': self.users_table, 'is_caching': True}
        self.SHARED_CASE_KWARGS = {**self.DYNAMODB_CASE_KWARGS, 'primary_key_name': 'accountId'}

    def test_set_get_fields_with_primary_index(self):
        from tests.tests_query_operations.cases_shared import test_set_get_fields_with_primary_index
        test_set_get_fields_with_primary_index(**self.SHARED_CASE_KWARGS)

    def test_set_get_fields_with_secondary_index(self):
        from tests.tests_query_operations.cases_dynamodb import test_set_get_fields_with_secondary_index
        test_set_get_fields_with_secondary_index(**self.DYNAMODB_CASE_KWARGS)

    def test_set_get_fields_with_overriding_names(self):
        from tests.tests_query_operations.cases_dynamodb import test_set_get_fields_with_overriding_names
        test_set_get_fields_with_overriding_names(**self.DYNAMODB_CASE_KWARGS)

    def test_set_get_fields_with_multi_selectors(self):
        from tests.tests_query_operations.cases_dynamodb import test_set_get_fields_with_multi_selectors
        test_set_get_fields_with_multi_selectors(**self.DYNAMODB_CASE_KWARGS)

    def test_set_get_multiple_fields_with_special_inner_keys(self):
        from tests.tests_query_operations.cases_dynamodb import test_set_get_multiple_fields_with_special_inner_keys
        test_set_get_multiple_fields_with_special_inner_keys(**self.DYNAMODB_CASE_KWARGS)


if __name__ == '__main__':
    unittest.main()
