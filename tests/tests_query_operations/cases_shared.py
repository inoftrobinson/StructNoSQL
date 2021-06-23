import random
import unittest
from uuid import uuid4
from typing import Dict, Any

from StructNoSQL import FieldSetter, FieldGetter, DynamoDBCachingTable, InoftVocalEngineCachingTable, DynamoDBBasicTable
from StructNoSQL.middlewares.inoft_vocal_engine.inoft_vocal_engine_basic_table import InoftVocalEngineBasicTable
from tests.components.playground_table_clients import TEST_ACCOUNT_ID, TEST_ACCOUNT_USERNAME


def test_set_get_fields_with_primary_index(
        self: unittest.TestCase, users_table: DynamoDBBasicTable or DynamoDBCachingTable or InoftVocalEngineBasicTable or InoftVocalEngineCachingTable,
        primary_key_name: str, is_caching: bool
):
    if is_caching is True:
        users_table.clear_cached_data_and_pending_operations()

    field1_random_value: str = f"field1_{uuid4()}"
    field2_random_value: str = f"field2_{uuid4()}"

    set_update_success: bool = users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
        FieldSetter(field_path='fieldOne', value_to_set=field1_random_value),
        FieldSetter(field_path='fieldTwo', value_to_set=field2_random_value)
    ])
    self.assertTrue(set_update_success)

    if is_caching is True:
        update_commit_success: bool = users_table.commit_operations()
        self.assertTrue(update_commit_success)

    retrieved_values = users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='(fieldOne, fieldTwo)')
    self.assertEqual(retrieved_values, {
        'fieldOne': (
            field1_random_value if is_caching is not True else
            {'fromCache': True, 'value': field1_random_value}
        ),
        'fieldTwo': (
            field2_random_value if is_caching is not True else
            {'fromCache': True, 'value': field2_random_value}
        )
    })

    single_field_not_primary_key = users_table.query_field(key_value=TEST_ACCOUNT_ID, field_path='fieldOne')
    self.assertEqual(single_field_not_primary_key, {TEST_ACCOUNT_ID: (
        field1_random_value if is_caching is not True else
        {'fromCache': True, 'value': field1_random_value}
    )})

    single_field_primary_key = users_table.query_field(key_value=TEST_ACCOUNT_ID, field_path=f'{primary_key_name}')
    self.assertEqual(single_field_primary_key, {TEST_ACCOUNT_ID: (
        TEST_ACCOUNT_ID if is_caching is not True else
        {'fromCache': True, 'value': TEST_ACCOUNT_ID}
    )})

    multiple_fields_without_primary_key = users_table.query_field(
        key_value=TEST_ACCOUNT_ID, field_path='(fieldOne, fieldTwo)'
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            'fieldOne': (
                field1_random_value if is_caching is not True else
                {'fromCache': True, 'value': field1_random_value}
            ),
            'fieldTwo': (
                field2_random_value if is_caching is not True else
                {'fromCache': True, 'value': field2_random_value}
            )
        }}, multiple_fields_without_primary_key
    )

    multiple_fields_with_primary_key = users_table.query_field(
        key_value=TEST_ACCOUNT_ID, field_path=f'({primary_key_name}, fieldOne, fieldTwo)'
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            f'{primary_key_name}': (
                TEST_ACCOUNT_ID if is_caching is not True else
                {'fromCache': True, 'value': TEST_ACCOUNT_ID}
            ),
            'fieldOne': (
                field1_random_value if is_caching is not True else
                {'fromCache': True, 'value': field1_random_value}
            ),
            'fieldTwo': (
                field2_random_value if is_caching is not True else
                {'fromCache': True, 'value': field2_random_value}
            )
        }}, multiple_fields_with_primary_key
    )
