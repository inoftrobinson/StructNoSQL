import unittest
from typing import Dict, Any
from uuid import uuid4

from StructNoSQL import FieldSetter, DynamoDBCachingTable, DynamoDBBasicTable, FieldGetter
from tests.components.playground_table_clients import TEST_ACCOUNT_USERNAME, TEST_ACCOUNT_ID


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

    single_field_not_primary_key, query_metadata = users_table.query_field(key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='fieldOne')
    self.assertEqual({TEST_ACCOUNT_ID: (
        field_one_random_value if is_caching is not True else
        {'fromCache': False, 'value': field_one_random_value}
    )}, single_field_not_primary_key)

    single_field_primary_key, query_metadata = users_table.query_field(key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='accountId')
    self.assertEqual({TEST_ACCOUNT_ID: (
        TEST_ACCOUNT_ID if is_caching is not True else
        {'fromCache': False, 'value': TEST_ACCOUNT_ID}
    )}, single_field_primary_key)

    multiple_fields_without_primary_key, query_metadata = users_table.query_field(
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

    multiple_fields_with_primary_key, query_metadata = users_table.query_field(
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

def test_set_get_fields_with_overriding_names(
        self: unittest.TestCase, users_table: DynamoDBBasicTable or DynamoDBCachingTable, is_caching: bool
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

    retrieved_items_values, query_metadata = users_table.query_multiple_fields(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username', getters={
            'item1-value': FieldGetter(field_path='container.{{containerKey}}.fieldOne', query_kwargs={'containerKey': "item1"}),
            'item2-value': FieldGetter(field_path='container.{{containerKey}}.fieldOne', query_kwargs={'containerKey': "item2"}),
        }
    )
    retrieved_items_values: Dict[str, Any]
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
        self: unittest.TestCase, users_table: DynamoDBBasicTable or DynamoDBCachingTable, is_caching: bool
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

    retrieved_items_values, query_metadata = users_table.query_field(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='(fieldOne, fieldTwo)'
    )
    retrieved_items_values: Dict[str, Any]
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

    retrieved_items_values, query_metadata = users_table.query_multiple_fields(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username',
        getters={'item': FieldGetter(field_path='(fieldOne, fieldTwo)')}
    )
    retrieved_items_values: Dict[str, Any]
    print(retrieved_items_values)


def test_set_get_multiple_fields_with_special_inner_keys(
        self: unittest.TestCase, users_table: DynamoDBBasicTable or DynamoDBCachingTable, is_caching: bool
):
    """
    This test the client usage of '__PRIMARY_KEY__' as a getter key in a query_multiple_fields operation on a secondary index.
    The _inner_query_fields_secondary_index will be triggered to handle the requested, in our test case, we will not request
    the primary index, which will be added to the existing requested fields, by default with '__PRIMARY_KEY__' as the getter
    key, but since it is already used by the client, we should normally start to add random letters to '__PRIMARY_KEY__'
    until a free index key is found.
    """

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

    retrieved_items_values, query_metadata = users_table.query_multiple_fields(
        key_value=TEST_ACCOUNT_USERNAME, index_name='username',
        getters={
            '__PRIMARY_KEY__': FieldGetter(field_path='fieldOne'),
            'fieldTwo': FieldGetter(field_path='fieldTwo'),
        }
    )
    self.assertEqual({
        TEST_ACCOUNT_ID: {
            '__PRIMARY_KEY__': (
                field1_random_value if is_caching is not True else
                {'fromCache': False, 'value': field1_random_value}
            ),
            'fieldTwo': (
                field2_random_value if is_caching is not True else
                {'fromCache': False, 'value': field2_random_value}
            )
        }}, retrieved_items_values
    )
