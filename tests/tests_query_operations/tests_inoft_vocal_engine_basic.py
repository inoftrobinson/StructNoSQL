import unittest

from tests.tests_caching_table.table_models import InoftVocalEngineUsersCachingTable
from tests.tests_query_operations.table_models import InoftVocalEngineTableModel


class TestsInoftVocalEngineCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        # self.users_table = InoftVocalEngineUsersBasicTable(data_model=InoftVocalEngineTableModel)
        self.users_table = InoftVocalEngineUsersCachingTable(data_model=InoftVocalEngineTableModel)
        self.users_table.debug = True

        self.SHARED_CASE_KWARGS = {'self': self, 'users_table': self.users_table, 'primary_key_name': 'accountProjectUserId', 'is_caching': True}

    def test_set_get_fields_with_primary_index(self):
        from tests.tests_query_operations.cases_shared import test_set_get_fields_with_primary_index
        test_set_get_fields_with_primary_index(**self.SHARED_CASE_KWARGS)


if __name__ == '__main__':
    unittest.main()
