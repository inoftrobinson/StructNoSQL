import random
import unittest
from uuid import uuid4
from typing import Dict, Any

from StructNoSQL import BaseField, TableDataModel, MapModel, FieldSetter, FieldGetter, DynamoDBCachingTable, InoftVocalEngineCachingTable
from tests.tests_caching_table.caching_users_table import TEST_ACCOUNT_ID, TEST_ACCOUNT_USERNAME


def test_set_get_fields_with_primary_index(self: unittest.TestCase, users_table: DynamoDBCachingTable or InoftVocalEngineCachingTable, primary_key_name: str):
    users_table.clear_cached_data_and_pending_operations()
    random_field_value_one = random.randint(0, 100)
    random_field_value_two = random.randint(100, 200)

    set_update_success: bool = users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
        FieldSetter(field_path='simpleValue', value_to_set=random_field_value_one),
        FieldSetter(field_path='simpleValue2', value_to_set=random_field_value_two)
    ])
    self.assertTrue(set_update_success)

    update_commit_success: bool = users_table.commit_operations()
    self.assertTrue(update_commit_success)

    retrieved_values = users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='(simpleValue, simpleValue2)')
    self.assertEqual(retrieved_values, {
        'simpleValue': {'fromCache': True, 'value': random_field_value_one},
        'simpleValue2': {'fromCache': True, 'value': random_field_value_two}
    })

    single_field_not_primary_key = users_table.query_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
    self.assertEqual({TEST_ACCOUNT_ID: {'fromCache': True, 'value': random_field_value_one}}, single_field_not_primary_key)

    single_field_primary_key = users_table.query_field(key_value=TEST_ACCOUNT_ID, field_path=f'{primary_key_name}')
    self.assertEqual({TEST_ACCOUNT_ID: {'fromCache': True, 'value': TEST_ACCOUNT_ID}}, single_field_primary_key)

    multiple_fields_without_primary_key = users_table.query_field(
        key_value=TEST_ACCOUNT_ID, field_path='(simpleValue, simpleValue2)'
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            'simpleValue': {'fromCache': True, 'value': random_field_value_one},
            'simpleValue2': {'fromCache': True, 'value': random_field_value_two}
        }}, multiple_fields_without_primary_key
    )

    multiple_fields_with_primary_key = users_table.query_field(
        key_value=TEST_ACCOUNT_ID, field_path=f'({primary_key_name}, simpleValue, simpleValue2)'
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            f'{primary_key_name}': {'fromCache': True, 'value': TEST_ACCOUNT_ID},
            'simpleValue': {'fromCache': True, 'value': random_field_value_one},
            'simpleValue2': {'fromCache': True, 'value': random_field_value_two}
        }}, multiple_fields_with_primary_key
    )

def test_set_get_fields_with_overriding_names(self: unittest.TestCase, users_table: DynamoDBCachingTable or InoftVocalEngineCachingTable, primary_key_name: str):
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
            'item1-value': {'fromCache': False, 'value': item1_field1_random_value},
            'item2-value': {'fromCache': False, 'value': item2_field1_random_value}
        }}, retrieved_items_values
    )
