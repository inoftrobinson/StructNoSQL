import random
import unittest
from uuid import uuid4
from typing import Dict, Any

from StructNoSQL import FieldSetter, FieldGetter, DynamoDBCachingTable, InoftVocalEngineCachingTable, DynamoDBBasicTable
from StructNoSQL.middlewares.inoft_vocal_engine.inoft_vocal_engine_basic_table import InoftVocalEngineBasicTable
from tests.tests_caching_table.caching_users_table import TEST_ACCOUNT_ID, TEST_ACCOUNT_USERNAME


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
    self.assertEqual({TEST_ACCOUNT_ID: {'fromCache': True, 'value': field1_random_value}}, single_field_not_primary_key)

    single_field_primary_key = users_table.query_field(key_value=TEST_ACCOUNT_ID, field_path=f'{primary_key_name}')
    self.assertEqual({TEST_ACCOUNT_ID: {'fromCache': True, 'value': TEST_ACCOUNT_ID}}, single_field_primary_key)

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

def test_set_get_fields_with_overriding_names(
        self: unittest.TestCase, users_table: DynamoDBBasicTable or DynamoDBCachingTable or InoftVocalEngineBasicTable or InoftVocalEngineCachingTable,
        primary_key_name: str, is_caching: bool
):
    item1_field1_random_value: str = f"item1_field1_${uuid4()}"
    item2_field1_random_value: str = f"item2_field1_${uuid4()}"
    set_update_success: bool = users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
        FieldSetter(
            field_path='container.{{containerKey}}',
            query_kwargs={'containerKey': "item1"},
            value_to_set={'fieldOne': item1_field1_random_value}
        ),
        FieldSetter(
            field_path='container.{{containerKey}}',
            query_kwargs={'containerKey': "item2"},
            value_to_set={'fieldOne': item2_field1_random_value}
        )
    ])
    self.assertTrue(set_update_success)

    if is_caching is True:
        commit_success: bool = users_table.commit_operations()
        self.assertTrue(commit_success)

    retrieved_items_values: Dict[str, Any] = users_table.query_multiple_fields(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username', getters={
            'item1-value': FieldGetter(field_path='container.{{containerKey}}.fieldOne', query_kwargs={'containerKey': "item1"}),
            'item2-value': FieldGetter(field_path='container.{{containerKey}}.fieldOne', query_kwargs={'containerKey': "item2"}),
        }
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            'item1-value': (
                item1_field1_random_value if is_caching is not True else
                {'fromCache': False, 'value': item1_field1_random_value}
            ),
            'item2-value': (
                item2_field1_random_value if is_caching is not True else
                {'fromCache': False, 'value': item2_field1_random_value}
            )
        }}, retrieved_items_values
    )

def test_set_get_fields_with_multi_selectors(
        self: unittest.TestCase, users_table: DynamoDBBasicTable or DynamoDBCachingTable or InoftVocalEngineBasicTable or InoftVocalEngineCachingTable,
        primary_key_name: str, is_caching: bool
):
    field1_random_value: str = f"field1_${uuid4()}"
    field2_random_value: str = f"field2_${uuid4()}"
    set_update_success: bool = users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
        FieldSetter(field_path='fieldOne', value_to_set=field1_random_value),
        FieldSetter(field_path='fieldTwo', value_to_set=field2_random_value)
    ])
    self.assertTrue(set_update_success)

    if is_caching is True:
        commit_success: bool = users_table.commit_operations()
        self.assertTrue(commit_success)

    retrieved_items_values: Dict[str, Any] = users_table.query_field(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='(fieldOne, fieldTwo)'
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            'fieldOne': (
                field1_random_value if is_caching is not True else
                {'fromCache': False, 'value': field1_random_value}
            ),
            'fieldTwo': (
                field2_random_value if is_caching is not True else
                {'fromCache': False, 'value': field2_random_value}
            )
        }}, retrieved_items_values
    )

    retrieved_items_values: Dict[str, Any] = users_table.query_multiple_fields(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username',
        getters={'item': FieldGetter(field_path='(fieldOne, fieldTwo)')}
    )
    print(retrieved_items_values)
