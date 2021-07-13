import unittest

from tests.components.playground_table_clients import PlaygroundDynamoDBCachingTable
from tests.tests_update_field_return_old.table_models import DynamoDBTableModel


class TestsDynamoDBCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBCachingTable(data_model=DynamoDBTableModel)
        self.users_table.debug = True

        self.DYNAMODB_CASE_KWARGS = {'self': self, 'users_table': self.users_table, 'is_caching': True}
        self.SHARED_CASE_KWARGS = {**self.DYNAMODB_CASE_KWARGS, 'primary_key_name': 'accountId'}

    def test_update_field_return_old(self):
        from tests.tests_update_field_return_old.cases_shared import test_update_field_return_old
        test_update_field_return_old(**self.SHARED_CASE_KWARGS)