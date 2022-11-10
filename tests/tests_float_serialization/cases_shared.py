import random
import unittest
from typing import Optional, Union, Dict, Any

from StructNoSQL import DynamoDBBasicTable, DynamoDBCachingTable, FieldSetter,  FieldGetter
from tests.components.playground_table_clients import TEST_ACCOUNT_ID


def test_get_simple_float_field(
        self: unittest.TestCase,
        table_client: Union[DynamoDBBasicTable, DynamoDBCachingTable],
        is_caching: bool, primary_key_name: str
):
    simple_random_float_value: float = random.randint(1, 10000) / 100
    simple_float_update_success: bool = table_client.update_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleFloatField', value_to_set=simple_random_float_value
    )
    self.assertTrue(simple_float_update_success)
    if is_caching is True:
        self.assertTrue(table_client.commit_operations())
        table_client.clear_cached_data()

    retrieved_float_value: Optional[float] = table_client.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleFloatField'
    )
    self.assertEqual((
        simple_random_float_value if is_caching is not True else
        {'value': simple_random_float_value, 'fromCache': False}
    ), retrieved_float_value)


def test_get_multiple_float_fields(
        self: unittest.TestCase,
        table_client: Union[DynamoDBBasicTable, DynamoDBCachingTable],
        is_caching: bool, primary_key_name: str
):
    container_field_one_random_text_value: float = random.randint(1, 10000) / 100
    container_field_two_random_text_value: float = random.randint(10001, 20000) / 100
    first_table_container_fields_update_success: bool = table_client.update_multiple_fields(
        key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='floatsContainer.floatOne', value_to_set=container_field_one_random_text_value),
            FieldSetter(field_path='floatsContainer.floatTwo', value_to_set=container_field_two_random_text_value)
        ]
    )
    self.assertTrue(first_table_container_fields_update_success)

    if is_caching is True:
        self.assertTrue(table_client.commit_operations())
        table_client.clear_cached_data()

    second_table_retrieved_container_fields_without_data_validation: Dict[str, Optional[Any]] = table_client.get_multiple_fields(
        key_value=TEST_ACCOUNT_ID, getters={
            'one': FieldGetter(field_path='floatsContainer.floatOne'),
            'two': FieldGetter(field_path='floatsContainer.floatTwo'),
        }
    )
    self.assertEqual({
        'one': (
            container_field_one_random_text_value if is_caching is not True else
            {'value': container_field_one_random_text_value, 'fromCache': False}
        ),
        'two': (
            container_field_two_random_text_value if is_caching is not True else
            {'value': container_field_two_random_text_value, 'fromCache': False}
        )
    }, second_table_retrieved_container_fields_without_data_validation)


def test_remove_simple_float_field(
        self: unittest.TestCase,
        table_client: Union[DynamoDBBasicTable, DynamoDBCachingTable],
        is_caching: bool, primary_key_name: str
):
    simple_random_float_value: float = random.randint(1, 10000) / 100
    simple_float_field_update_success: bool = table_client.update_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleFloatField', value_to_set=simple_random_float_value
    )
    self.assertTrue(simple_float_field_update_success)
    if is_caching is True:
        self.assertTrue(table_client.commit_operations())
        table_client.clear_cached_data()

    removed_simple_float_field_value: Optional[float] = table_client.remove_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleFloatField'
    )
    self.assertEqual((
        simple_random_float_value if is_caching is not True else
        {'value': simple_random_float_value, 'fromCache': False}
    ), removed_simple_float_field_value)
