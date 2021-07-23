import unittest

from tests.components.playground_table_clients import PlaygroundInoftVocalEngineCachingTable
from tests.tests_custom_field_name.table_models import ExternalDynamoDBApiTableModel


class TestsExternalDynamoDBApiCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundInoftVocalEngineCachingTable(data_model=ExternalDynamoDBApiTableModel)
        self.users_table.debug = True

        self.SHARED_CASE_KWARGS = {'self': self, 'users_table': self.users_table, 'primary_key_name': 'accountProjectTableKeyId', 'is_caching': True}

    def test_set_update_field_with_custom_name(self):
        from tests.tests_custom_field_name.cases_shared import test_set_update_field_with_custom_name
        test_set_update_field_with_custom_name(**self.SHARED_CASE_KWARGS)


if __name__ == '__main__':
    unittest.main()
