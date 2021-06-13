import random
import unittest

from StructNoSQL import FieldSetter, DynamoDBCachingTable, DynamoDBBasicTable
from tests.users_table import TEST_ACCOUNT_USERNAME, TEST_ACCOUNT_ID


def test_set_get_fields_with_secondary_index(self: unittest.TestCase, users_table: DynamoDBBasicTable or DynamoDBCachingTable, is_caching: bool):
    if is_caching is True:
        users_table.clear_cached_data_and_pending_operations()
        
    random_field_value_one = random.randint(0, 100)
    random_field_value_two = random.randint(100, 200)

    set_update_success: bool = users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
        FieldSetter(field_path='simpleValue', value_to_set=random_field_value_one),
        FieldSetter(field_path='simpleValue2', value_to_set=random_field_value_two)
    ])
    self.assertTrue(set_update_success)

    if is_caching is True:
        update_commit_success: bool = users_table.commit_operations()
        self.assertTrue(update_commit_success)

    retrieved_values = users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='(simpleValue, simpleValue2)')
    self.assertEqual({
        'simpleValue': (
            random_field_value_one if is_caching is not True else
            {'fromCache': True, 'value': random_field_value_one}
        ),
        'simpleValue2': (
            random_field_value_two if is_caching is not True else
            {'fromCache': True, 'value': random_field_value_two}
        )
    }, retrieved_values)

    single_field_not_primary_key = users_table.query_field(key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='simpleValue')
    self.assertEqual({TEST_ACCOUNT_ID: (
        random_field_value_one if is_caching is not True else
        {'fromCache': False, 'value': random_field_value_one}
    )}, single_field_not_primary_key)

    single_field_primary_key = users_table.query_field(key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='accountId')
    self.assertEqual({TEST_ACCOUNT_ID: (
        TEST_ACCOUNT_ID if is_caching is not True else
        {'fromCache': False, 'value': TEST_ACCOUNT_ID}
    )}, single_field_primary_key)

    multiple_fields_without_primary_key = users_table.query_field(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='(simpleValue, simpleValue2)'
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            'simpleValue': (
                random_field_value_one if is_caching is not True else
                {'fromCache': False, 'value': random_field_value_one}
            ),
            'simpleValue2': (
                random_field_value_two if is_caching is not True else
                {'fromCache': False, 'value': random_field_value_two}
            )
        }}, multiple_fields_without_primary_key
    )

    multiple_fields_with_primary_key = users_table.query_field(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username',
        field_path='(accountId, simpleValue, simpleValue2)'
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            'accountId': (
                TEST_ACCOUNT_ID if is_caching is not True else
                {'fromCache': False, 'value': TEST_ACCOUNT_ID}
            ),
            'simpleValue': (
                random_field_value_one if is_caching is not True else
                {'fromCache': False, 'value': random_field_value_one}
            ),
            'simpleValue2': (
                random_field_value_two if is_caching is not True else
                {'fromCache': False, 'value': random_field_value_two}
            )
        }}, multiple_fields_with_primary_key
    )