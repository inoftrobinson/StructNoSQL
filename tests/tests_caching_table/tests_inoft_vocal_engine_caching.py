import unittest

from tests.components.playground_table_clients import PlaygroundInoftVocalEngineCachingTable
from tests.tests_caching_table.table_models import InoftVocalEngineTableModel


class TestInoftVocalEngineCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundInoftVocalEngineCachingTable(data_model=InoftVocalEngineTableModel)
        self.users_table.debug = True

    def test_simple_get_field(self):
        from tests.tests_caching_table.cases_shared import test_simple_get_field
        test_simple_get_field(self, users_table=self.users_table)

    def test_set_then_get_field_from_cache(self):
        from tests.tests_caching_table.cases_shared import test_set_then_get_field_from_cache
        test_set_then_get_field_from_cache(self, users_table=self.users_table)

    def test_set_then_get_multiple_fields(self):
        from tests.tests_caching_table.cases_shared import test_set_then_get_multiple_fields
        test_set_then_get_multiple_fields(self, users_table=self.users_table)

    def test_set_then_get_pack_values_with_one_of_them_present_in_cache(self):
        from tests.tests_caching_table.cases_shared import test_set_then_get_pack_values_with_one_of_them_present_in_cache
        test_set_then_get_pack_values_with_one_of_them_present_in_cache(self, users_table=self.users_table)

    def test_debug_simple_set_commit_then_get_field_from_database(self):
        from tests.tests_caching_table.cases_shared import test_debug_simple_set_commit_then_get_field_from_database
        test_debug_simple_set_commit_then_get_field_from_database(self, users_table=self.users_table)

    def test_update_multiple_fields(self):
        from tests.tests_caching_table.cases_shared import test_update_multiple_fields
        test_update_multiple_fields(self, users_table=self.users_table)

    def test_set_delete_field(self):
        from tests.tests_caching_table.cases_shared import test_set_delete_field
        test_set_delete_field(self, users_table=self.users_table)

    def test_set_remove_field(self):
        from tests.tests_caching_table.cases_shared import test_set_remove_field
        test_set_remove_field(self, users_table=self.users_table)

    def test_dict_data_unpacking(self):
        from tests.tests_caching_table.cases_shared import test_dict_data_unpacking
        test_dict_data_unpacking(self, users_table=self.users_table)

    def test_list_data_unpacking(self):
        from tests.tests_caching_table.cases_shared import test_list_data_unpacking
        test_list_data_unpacking(self, users_table=self.users_table)

    def test_set_remove_multi_selector_field_and_field_unpacking(self):
        from tests.tests_caching_table.cases_shared import test_set_remove_multi_selector_field_and_field_unpacking
        test_set_remove_multi_selector_field_and_field_unpacking(self, users_table=self.users_table)

    def test_set_delete_multiple_fields(self):
        from tests.tests_caching_table.cases_shared import test_set_delete_multiple_fields
        test_set_delete_multiple_fields(self, users_table=self.users_table)

    def test_set_remove_multiple_fields(self):
        from tests.tests_caching_table.cases_shared import test_set_remove_multiple_fields
        test_set_remove_multiple_fields(self, users_table=self.users_table)

    def test_set_get_fields_with_primary_index(self):
        from tests.tests_query_operations.cases_shared import test_set_get_fields_with_primary_index
        test_set_get_fields_with_primary_index(self, users_table=self.users_table, primary_key_name='accountProjectUserId', is_caching=True)


if __name__ == '__main__':
    unittest.main()