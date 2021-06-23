import random
import unittest
from uuid import uuid4

from StructNoSQL import FieldSetter, DynamoDBCachingTable, DynamoDBBasicTable
from tests.users_table import TEST_ACCOUNT_USERNAME, TEST_ACCOUNT_ID


def test_set_get_fields_with_secondary_index(self: unittest.TestCase, users_table: DynamoDBBasicTable or DynamoDBCachingTable, is_caching: bool):
    if is_caching is True:
        users_table.clear_cached_data_and_pending_operations()
        
    field_one_random_value = f"field1_{uuid4()}"
    field_two_random_value = f"field2_{uuid4()}"

    set_update_success: bool = users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
        FieldSetter(field_path='fieldOne', value_to_set=field_one_random_value),
        FieldSetter(field_path='fieldTwo', value_to_set=field_two_random_value)
    ])
    self.assertTrue(set_update_success)

    if is_caching is True:
        update_commit_success: bool = users_table.commit_operations()
        self.assertTrue(update_commit_success)

    retrieved_values = users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='(fieldOne, fieldTwo)')
    self.assertEqual({
        'fieldOne': (
            field_one_random_value if is_caching is not True else
            {'fromCache': True, 'value': field_one_random_value}
        ),
        'fieldTwo': (
            field_two_random_value if is_caching is not True else
            {'fromCache': True, 'value': field_two_random_value}
        )
    }, retrieved_values)

    single_field_not_primary_key = users_table.query_field(key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='fieldOne')
    self.assertEqual({TEST_ACCOUNT_ID: (
        field_one_random_value if is_caching is not True else
        {'fromCache': False, 'value': field_one_random_value}
    )}, single_field_not_primary_key)

    single_field_primary_key = users_table.query_field(key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='accountId')
    self.assertEqual({TEST_ACCOUNT_ID: (
        TEST_ACCOUNT_ID if is_caching is not True else
        {'fromCache': False, 'value': TEST_ACCOUNT_ID}
    )}, single_field_primary_key)

    multiple_fields_without_primary_key = users_table.query_field(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='(fieldOne, fieldTwo)'
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            'fieldOne': (
                field_one_random_value if is_caching is not True else
                {'fromCache': False, 'value': field_one_random_value}
            ),
            'fieldTwo': (
                field_two_random_value if is_caching is not True else
                {'fromCache': False, 'value': field_two_random_value}
            )
        }}, multiple_fields_without_primary_key
    )

    multiple_fields_with_primary_key = users_table.query_field(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username',
        field_path='(accountId, fieldOne, fieldTwo)'
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            'accountId': (
                TEST_ACCOUNT_ID if is_caching is not True else
                {'fromCache': False, 'value': TEST_ACCOUNT_ID}
            ),
            'fieldOne': (
                field_one_random_value if is_caching is not True else
                {'fromCache': False, 'value': field_one_random_value}
            ),
            'fieldTwo': (
                field_two_random_value if is_caching is not True else
                {'fromCache': False, 'value': field_two_random_value}
            )
        }}, multiple_fields_with_primary_key
    )