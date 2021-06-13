import unittest

from StructNoSQL import BaseField
from tests.tests_caching_table.caching_users_table import InoftVocalEngineUsersCachingTable
from tests.tests_caching_table.test_query_operations.table_model import QueryOperationsBaseTableModel


class InoftVocalEngineTableModel(QueryOperationsBaseTableModel):
    accountProjectUserId = BaseField(name='accountProjectUserId', field_type=str, required=True)


class TestInoftVocalEngineCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = InoftVocalEngineUsersCachingTable(data_model=InoftVocalEngineTableModel)
        self.users_table.debug = True

    def test_set_get_fields_with_primary_index(self):
        from tests.tests_caching_table.test_query_operations.cases_shared import test_set_get_fields_with_primary_index
        test_set_get_fields_with_primary_index(self, users_table=self.users_table, primary_key_name='accountProjectUserId')

    def test_set_get_fields_with_overriding_names(self):
        from tests.tests_caching_table.test_query_operations.cases_shared import test_set_get_fields_with_overriding_names
        test_set_get_fields_with_overriding_names(self, users_table=self.users_table, primary_key_name='accountProjectUserId')


if __name__ == '__main__':
    unittest.main()
