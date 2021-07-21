import unittest

from tests.components.playground_table_clients import PlaygroundInoftVocalEngineCachingTable
from tests.tests_update_field_return_old.table_models import ExternalDynamoDBApiTableModel


class TestsExternalDynamoDBApiCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundInoftVocalEngineCachingTable(data_model=ExternalDynamoDBApiTableModel)
        self.users_table.debug = True

        self.SHARED_CASE_KWARGS = {'self': self, 'users_table': self.users_table, 'primary_key_name': 'accountProjectUserId', 'is_caching': True}

    def test_update_field_return_old(self):
        from tests.tests_update_field_return_old.cases_shared import test_update_field_return_old
        test_update_field_return_old(**self.SHARED_CASE_KWARGS)

    def test_update_multiple_fields_return_old(self):
        from tests.tests_update_field_return_old.cases_shared import test_update_multiple_fields_return_old
        test_update_multiple_fields_return_old(**self.SHARED_CASE_KWARGS)
