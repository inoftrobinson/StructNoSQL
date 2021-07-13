import unittest
from typing import Optional, Union
from uuid import uuid4

from StructNoSQL import DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineCachingTable
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
        users_table.clear_cached_data_and_pending_operations()

    simple_text_field_second_random_value: str = f"simpleTextField_secondRandomValue_{uuid4()}"
    second_random_value_update_success, field_old_value = users_table.update_field_return_old(
        key_value=TEST_ACCOUNT_ID, field_path='simpleTextField', value_to_set=simple_text_field_second_random_value
    )
    self.assertTrue(second_random_value_update_success)
    self.assertEqual(simple_text_field_first_random_value, field_old_value)

    retrieved_second_random_value: Optional[str] = users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleTextField')
    self.assertEqual(simple_text_field_second_random_value, retrieved_second_random_value)


def test_update_field_with_multi_selectors_return_old(
        self: unittest.TestCase, users_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        primary_key_name: str, is_caching: bool
):
    simple_text_field_first_random_value: str = f"simpleTextField_firstRandomValue_{uuid4()}"
    first_random_value_update_success: bool = users_table.update_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleTextField', value_to_set=simple_text_field_first_random_value
    )
    self.assertTrue(first_random_value_update_success)

    if is_caching is True:
        users_table.clear_cached_data_and_pending_operations()

    simple_text_field_second_random_value: str = f"simpleTextField_secondRandomValue_{uuid4()}"
    second_random_value_update_success, field_old_value = users_table.update_field_return_old(
        key_value=TEST_ACCOUNT_ID, field_path='simpleTextField', value_to_set=simple_text_field_second_random_value
    )
    self.assertTrue(second_random_value_update_success)
    self.assertEqual(simple_text_field_first_random_value, field_old_value)

    retrieved_second_random_value: Optional[str] = users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleTextField')
    self.assertEqual(simple_text_field_second_random_value, retrieved_second_random_value)


def test_update_multiple_fields_return_old(
        self: unittest.TestCase, users_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        primary_key_name: str, is_caching: bool
):
    simple_text_field_first_random_value: str = f"simpleTextField_firstRandomValue_{uuid4()}"
    first_random_value_update_success: bool = users_table.update_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleTextField', value_to_set=simple_text_field_first_random_value
    )
    self.assertTrue(first_random_value_update_success)

    if is_caching is True:
        users_table.clear_cached_data_and_pending_operations()

    simple_text_field_second_random_value: str = f"simpleTextField_secondRandomValue_{uuid4()}"
    second_random_value_update_success, field_old_value = users_table.update_field_return_old(
        key_value=TEST_ACCOUNT_ID, field_path='simpleTextField', value_to_set=simple_text_field_second_random_value
    )
    self.assertTrue(second_random_value_update_success)
    self.assertEqual(simple_text_field_first_random_value, field_old_value)

    retrieved_second_random_value: Optional[str] = users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleTextField')
    self.assertEqual(simple_text_field_second_random_value, retrieved_second_random_value)
