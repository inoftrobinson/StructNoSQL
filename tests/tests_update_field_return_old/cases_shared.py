import unittest
from typing import Optional, Union, Dict, Any
from uuid import uuid4

from StructNoSQL import DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineCachingTable, FieldSetter
from StructNoSQL.middlewares.inoft_vocal_engine.inoft_vocal_engine_basic_table import InoftVocalEngineBasicTable
from tests.components.playground_table_clients import TEST_ACCOUNT_ID


def test_update_field_return_old(
        self: unittest.TestCase, users_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        primary_key_name: str, is_caching: bool
):
    simple_text_field_first_random_value: str = f"simpleTextField_firstRandomValue_{uuid4()}"
    first_random_value_update_success: bool = users_table.update_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleTextField', value_to_set=simple_text_field_first_random_value
    )
    self.assertTrue(first_random_value_update_success)

    if is_caching is True:
        operations_commit_success: bool = users_table.commit_operations()
        self.assertTrue(operations_commit_success)
        users_table.clear_cached_data()

    simple_text_field_second_random_value: str = f"simpleTextField_secondRandomValue_{uuid4()}"
    second_random_value_update_success, field_old_value = users_table.update_field_return_old(
        key_value=TEST_ACCOUNT_ID, field_path='simpleTextField', value_to_set=simple_text_field_second_random_value
    )
    self.assertTrue(second_random_value_update_success)
    self.assertEqual((
        simple_text_field_first_random_value if is_caching is not True else
        {'fromCache': False, 'value': simple_text_field_first_random_value}
    ), field_old_value)

    if is_caching is True:
        operations_commit_success: bool = users_table.commit_operations()
        self.assertTrue(operations_commit_success)
        users_table.clear_cached_data()

    retrieved_second_random_value: Optional[str] = users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleTextField')
    self.assertEqual((
        simple_text_field_second_random_value if is_caching is not True else
        {'fromCache': False, 'value': simple_text_field_second_random_value}
    ), retrieved_second_random_value)


def test_update_multiple_fields_return_old(
        self: unittest.TestCase, users_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        primary_key_name: str, is_caching: bool
):
    container_field_one_first_random_value: str = f"container_fieldOne_firstRandomValue_{uuid4()}"
    container_field_two_first_random_value: str = f"container_fieldTwo_firstRandomValue_{uuid4()}"
    container_fields_first_update_success: bool = users_table.update_multiple_fields(
        key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='container.textFieldOne', value_to_set=container_field_one_first_random_value),
            FieldSetter(field_path='container.textFieldTwo', value_to_set=container_field_two_first_random_value)
        ]
    )
    self.assertTrue(container_fields_first_update_success)

    if is_caching is True:
        operations_commit_success: bool = users_table.commit_operations()
        self.assertTrue(operations_commit_success)
        users_table.clear_cached_data()

    container_field_one_second_random_value: str = f"container_fieldOne_secondRandomValue_{uuid4()}"
    container_field_two_second_random_value: str = f"container_fieldTwo_secondRandomValue_{uuid4()}"
    container_fields_second_update_success, field_old_value = users_table.update_multiple_fields_return_old(
        key_value=TEST_ACCOUNT_ID, setters={
            'fieldOne': FieldSetter(field_path='container.textFieldOne', value_to_set=container_field_one_second_random_value),
            'fieldTwo': FieldSetter(field_path='container.textFieldTwo', value_to_set=container_field_two_second_random_value)
        }
    )
    self.assertTrue(container_fields_second_update_success)
    self.assertEqual({
        'fieldOne': (
            container_field_one_first_random_value if is_caching is not True else
            {'fromCache': False, 'value': container_field_one_first_random_value}
        ),
        'fieldTwo': (
            container_field_two_first_random_value if is_caching is not True else
            {'fromCache': False, 'value': container_field_two_first_random_value}
        )
    }, field_old_value)

    retrieved_container_fields_second_random_values: Optional[Dict[str, Optional[Any]]] = users_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(textFieldOne, textFieldTwo)'
    )
    self.assertEqual({
        'textFieldOne': (
            container_field_one_second_random_value if is_caching is not True else
            {'fromCache': False, 'value': container_field_one_second_random_value}
        ),
        'textFieldTwo': (
            container_field_two_second_random_value if is_caching is not True else
            {'fromCache': False, 'value': container_field_two_second_random_value}
        )
    }, retrieved_container_fields_second_random_values)
