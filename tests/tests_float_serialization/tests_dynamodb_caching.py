import unittest

from tests.components.playground_table_clients import PlaygroundDynamoDBCachingTable
from tests.tests_float_serialization.table_models import DynamoDBTableModel


class TestsDynamoDBCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.table_client = PlaygroundDynamoDBCachingTable(data_model=DynamoDBTableModel)
        self.table_client.debug = True

        self.SHARED_CASE_KWARGS = {
            'self': self, 'table_client': self.table_client,
            'is_caching': True, 'primary_key_name': 'accountId'
        }

    def test_get_field(self):
        from tests.tests_float_serialization.cases_shared import test_get_simple_float_field
        test_get_simple_float_field(**self.SHARED_CASE_KWARGS)

    def test_get_multiple_float_fields(self):
        from tests.tests_float_serialization.cases_shared import test_get_multiple_float_fields
        test_get_multiple_float_fields(**self.SHARED_CASE_KWARGS)

    def test_remove_simple_float_field(self):
        from tests.tests_float_serialization.cases_shared import test_remove_simple_float_field
        test_remove_simple_float_field(**self.SHARED_CASE_KWARGS)
