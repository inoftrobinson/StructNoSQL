import random
import unittest

from StructNoSQL import BaseField
from tests.tests_caching_table.caching_users_table import CachingUsersTable, TEST_ACCOUNT_USERNAME, TEST_ACCOUNT_ID
from tests.tests_caching_table.shared_cases import TableModel


class DynamoDBTableModel(TableModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)


class TestDynamoDBCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = CachingUsersTable(data_model=DynamoDBTableModel)
        self.users_table.debug = True

    def test_simple_get_field(self):
        from tests.tests_caching_table.shared_cases import test_simple_get_field
        test_simple_get_field(self, users_table=self.users_table)

    def test_set_then_get_field_from_cache(self):
        from tests.tests_caching_table.shared_cases import test_set_then_get_field_from_cache
        test_set_then_get_field_from_cache(self, users_table=self.users_table)

    def test_set_then_get_multiple_fields(self):
        from tests.tests_caching_table.shared_cases import test_set_then_get_multiple_fields
        test_set_then_get_multiple_fields(self, users_table=self.users_table)

    def test_set_then_get_pack_values_with_one_of_them_present_in_cache(self):
        from tests.tests_caching_table.shared_cases import test_set_then_get_pack_values_with_one_of_them_present_in_cache
        test_set_then_get_pack_values_with_one_of_them_present_in_cache(self, users_table=self.users_table)

    def test_debug_simple_set_commit_then_get_field_from_database(self):
        from tests.tests_caching_table.shared_cases import test_debug_simple_set_commit_then_get_field_from_database
        test_debug_simple_set_commit_then_get_field_from_database(self, users_table=self.users_table)

    def test_update_multiple_fields(self):
        from tests.tests_caching_table.shared_cases import test_update_multiple_fields
        test_update_multiple_fields(self, users_table=self.users_table)

    def test_set_delete_field(self):
        from tests.tests_caching_table.shared_cases import test_set_delete_field
        test_set_delete_field(self, users_table=self.users_table)

    def test_set_remove_field(self):
        from tests.tests_caching_table.shared_cases import test_set_remove_field
        test_set_remove_field(self, users_table=self.users_table)

    def test_dict_data_unpacking(self):
        from tests.tests_caching_table.shared_cases import test_dict_data_unpacking
        test_dict_data_unpacking(self, users_table=self.users_table)

    def test_list_data_unpacking(self):
        from tests.tests_caching_table.shared_cases import test_list_data_unpacking
        test_list_data_unpacking(self, users_table=self.users_table)

    def test_set_remove_multi_selector_field_and_field_unpacking(self):
        from tests.tests_caching_table.shared_cases import test_set_remove_multi_selector_field_and_field_unpacking
        test_set_remove_multi_selector_field_and_field_unpacking(self, users_table=self.users_table)

    def test_set_delete_multiple_fields(self):
        from tests.tests_caching_table.shared_cases import test_set_delete_multiple_fields
        test_set_delete_multiple_fields(self, users_table=self.users_table)

    def test_set_remove_multiple_fields(self):
        from tests.tests_caching_table.shared_cases import test_set_remove_multiple_fields
        test_set_remove_multiple_fields(self, users_table=self.users_table)

    def test_set_get_fields_with_secondary_index(self):
        self.users_table.clear_cached_data_and_pending_operations()
        random_field_value_one = random.randint(0, 100)
        random_field_value_two = random.randint(100, 200)

        set_update_success: bool = self.users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='simpleValue', value_to_set=random_field_value_one),
            FieldSetter(field_path='simpleValue2', value_to_set=random_field_value_two)
        ])
        self.assertTrue(set_update_success)

        update_commit_success: bool = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        retrieved_values = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='(simpleValue, simpleValue2)')
        self.assertEqual(retrieved_values, {
            'simpleValue': {'fromCache': True, 'value': random_field_value_one},
            'simpleValue2': {'fromCache': True, 'value': random_field_value_two}
        })

        single_field_not_primary_key = self.users_table.query_field(key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='simpleValue')
        self.assertEqual({TEST_ACCOUNT_ID: {'fromCache': False, 'value': random_field_value_one}}, single_field_not_primary_key)

        single_field_primary_key = self.users_table.query_field(key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='accountId')
        self.assertEqual({TEST_ACCOUNT_ID: {'fromCache': False, 'value': TEST_ACCOUNT_ID}}, single_field_primary_key)

        multiple_fields_without_primary_key = self.users_table.query_field(
            key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='(simpleValue, simpleValue2)'
        )
        self.assertEqual({
            TEST_ACCOUNT_ID: {
                'simpleValue': {'fromCache': False, 'value': random_field_value_one},
                'simpleValue2': {'fromCache': False, 'value': random_field_value_two}
            }}, multiple_fields_without_primary_key
        )

        multiple_fields_with_primary_key = self.users_table.query_field(
            key_value=TEST_ACCOUNT_USERNAME, index_name='username',
            field_path='(accountId, simpleValue, simpleValue2)'
        )
        self.assertEqual({
            TEST_ACCOUNT_ID: {
                'accountId': {'fromCache': False, 'value': TEST_ACCOUNT_ID},
                'simpleValue': {'fromCache': False, 'value': random_field_value_one},
                'simpleValue2': {'fromCache': False, 'value': random_field_value_two}
            }}, multiple_fields_with_primary_key
        )


if __name__ == '__main__':
    unittest.main()
