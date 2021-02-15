import random
import unittest

from StructNoSQL import TableDataModel, BaseField
from tests.tests_caching_table.users_table import CachingUsersTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    simpleValue = BaseField(name='simpleValue', field_type=int, required=False)
    fieldToDelete = BaseField(name='fieldToDelete', field_type=int, required=False)
    fieldToRemove = BaseField(name='fieldToRemove', field_type=int, required=False)

class TestGetField(unittest.TestCase):
    def reset_table(self):
        self.users_table = CachingUsersTable(data_model=TableModel)
        self.users_table.debug = True

    def test_debug_simple_get_field(self):
        self.reset_table()

        first_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual(first_response_data['fromCache'], False)

        second_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual(second_response_data['fromCache'], True)

    def test_debug_simple_set_then_get_field_from_cache(self):
        self.reset_table()
        random_field_value = random.randint(0, 100)

        update_success = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue', value_to_set=random_field_value)
        self.assertTrue(update_success)

        retrieve_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual(retrieve_response_data['fromCache'], True)

    def test_debug_simple_set_commit_then_get_field_from_database(self):
        self.reset_table()
        random_field_value = random.randint(0, 100)

        update_success = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue', value_to_set=random_field_value)
        self.assertTrue(update_success)
        commit_success = self.users_table.commit_operations()
        self.assertTrue(commit_success)

        self.reset_table()
        retrieve_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual(retrieve_response_data['value'], random_field_value)
        self.assertEqual(retrieve_response_data['fromCache'], False)

    def test_set_delete_field(self):
        self.reset_table()
        random_field_value = random.randint(0, 100)

        update_success = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete', value_to_set=random_field_value)
        self.assertTrue(update_success)
        update_commit_success = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        delete_success = self.users_table.delete_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete')
        self.assertTrue(delete_success)
        delete_commit_success = self.users_table.commit_operations()
        self.assertTrue(delete_commit_success)

        retrieved_expected_empty_value_from_cache = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete')
        self.assertTrue(retrieved_expected_empty_value_from_cache['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_from_cache['value'])

        self.reset_table()
        retrieved_expected_empty_value_from_database = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete')
        self.assertFalse(retrieved_expected_empty_value_from_database['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_from_database['value'])

    def test_set_remove_field(self):
        self.reset_table()
        random_field_value = random.randint(0, 100)

        update_success = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove', value_to_set=random_field_value)
        self.assertTrue(update_success)
        update_commit_success = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        removed_value = self.users_table.remove_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove')
        self.assertEqual(removed_value, random_field_value)

        retrieved_expected_empty_value_from_cache = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove')
        self.assertTrue(retrieved_expected_empty_value_from_cache['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_from_cache['value'])

        self.reset_table()
        retrieved_expected_empty_value_from_database = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove')
        self.assertFalse(retrieved_expected_empty_value_from_database['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_from_database['value'])


if __name__ == '__main__':
    unittest.main()
