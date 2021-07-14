import unittest
from typing import Optional, Union, Dict, Any
from uuid import uuid4

from StructNoSQL import DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineCachingTable, FieldSetter, \
    FieldGetter, FieldRemover
from StructNoSQL.middlewares.inoft_vocal_engine.inoft_vocal_engine_basic_table import InoftVocalEngineBasicTable
from tests.components.playground_table_clients import TEST_ACCOUNT_ID


def test_get_field(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    simple_field_random_text_value: str = f"simpleField_randomTextValue_{uuid4()}"
    first_table_simple_field_update_success: bool = first_table.update_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', value_to_set=simple_field_random_text_value
    )
    self.assertTrue(first_table_simple_field_update_success)
    if is_caching is True:
        self.assertTrue(first_table.commit_operations())
        first_table.clear_cached_data()

    first_table_retrieved_simple_field: Optional[str] = first_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField'
    )
    self.assertEqual((
        simple_field_random_text_value if is_caching is not True else
        {'value': simple_field_random_text_value, 'fromCache': False}
    ), first_table_retrieved_simple_field)

    second_table_retrieved_simple_field: Optional[int] = second_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField'
    )
    self.assertEqual((
        None if is_caching is not True else
        {'value': None, 'fromCache': False}
    ), second_table_retrieved_simple_field)


def test_get_field_multi_selectors(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    container_field_one_random_text_value: str = f"container_fieldOne_randomTextValue_{uuid4()}"
    container_field_two_random_text_value: str = f"container_fieldTwo_randomTextValue_{uuid4()}"
    first_table_container_fields_update_success: bool = first_table.update_multiple_fields(
        key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='container.nestedFieldOne', value_to_set=container_field_one_random_text_value),
            FieldSetter(field_path='container.nestedFieldTwo', value_to_set=container_field_two_random_text_value)
        ]
    )
    self.assertTrue(first_table_container_fields_update_success)
    if is_caching is True:
        self.assertTrue(first_table.commit_operations())
        first_table.clear_cached_data()

    first_table_retrieved_container_fields: Dict[str, Optional[str]] = first_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)'
    )
    self.assertEqual({
        'nestedFieldOne': (
            container_field_one_random_text_value if is_caching is not True else
            {'value': container_field_one_random_text_value, 'fromCache': False}
        ),
        'nestedFieldTwo': (
            container_field_two_random_text_value if is_caching is not True else
            {'value': container_field_two_random_text_value, 'fromCache': False}
        )
    }, first_table_retrieved_container_fields)

    second_table_retrieved_container_fields: Dict[str, Optional[int]] = second_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)'
    )
    self.assertEqual({
        'nestedFieldOne': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        ),
        'nestedFieldTwo': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        )
    }, second_table_retrieved_container_fields)


def test_get_multiple_fields(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    container_field_one_random_text_value: str = f"container_fieldOne_randomTextValue_{uuid4()}"
    container_field_two_random_text_value: str = f"container_fieldTwo_randomTextValue_{uuid4()}"
    first_table_container_fields_update_success: bool = first_table.update_multiple_fields(
        key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='container.nestedFieldOne', value_to_set=container_field_one_random_text_value),
            FieldSetter(field_path='container.nestedFieldTwo', value_to_set=container_field_two_random_text_value)
        ]
    )
    self.assertTrue(first_table_container_fields_update_success)

    if is_caching is True:
        self.assertTrue(first_table.commit_operations())
        first_table.clear_cached_data()

    first_table_retrieved_container_fields: Dict[str, Optional[str]] = first_table.get_multiple_fields(
        key_value=TEST_ACCOUNT_ID, getters={
            'one': FieldGetter(field_path='container.nestedFieldOne'),
            'two': FieldGetter(field_path='container.nestedFieldTwo'),
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
    }, first_table_retrieved_container_fields)

    second_table_retrieved_container_fields: Dict[str, Optional[int]] = second_table.get_multiple_fields(
        key_value=TEST_ACCOUNT_ID, getters={
            'one': FieldGetter(field_path='container.nestedFieldOne'),
            'two': FieldGetter(field_path='container.nestedFieldTwo'),
        }
    )
    self.assertEqual({
        'one': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        ),
        'two': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        )
    }, second_table_retrieved_container_fields)


def test_remove_field(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    simple_field_random_text_value: str = f"simpleField_randomTextValue_{uuid4()}"
    first_table_simple_field_update_success: bool = first_table.update_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', value_to_set=simple_field_random_text_value
    )
    self.assertTrue(first_table_simple_field_update_success)
    if is_caching is True:
        self.assertTrue(first_table.commit_operations())
        first_table.clear_cached_data()

    first_table_retrieved_simple_field: Optional[str] = first_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField'
    )
    self.assertEqual((
        simple_field_random_text_value if is_caching is not True else
        {'value': simple_field_random_text_value, 'fromCache': False}
    ), first_table_retrieved_simple_field)

    second_table_retrieved_simple_field: Optional[int] = second_table.remove_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField'
    )
    self.assertEqual((
        None if is_caching is not True else
        {'value': None, 'fromCache': False}
    ), second_table_retrieved_simple_field)


def test_remove_field_multi_selectors(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    container_field_one_random_text_value: str = f"container_fieldOne_randomTextValue_{uuid4()}"
    container_field_two_random_text_value: str = f"container_fieldTwo_randomTextValue_{uuid4()}"
    first_table_container_fields_update_success: bool = first_table.update_multiple_fields(
        key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='container.nestedFieldOne', value_to_set=container_field_one_random_text_value),
            FieldSetter(field_path='container.nestedFieldTwo', value_to_set=container_field_two_random_text_value)
        ]
    )
    self.assertTrue(first_table_container_fields_update_success)
    if is_caching is True:
        self.assertTrue(first_table.commit_operations())
        first_table.clear_cached_data()

    first_table_retrieved_container_fields: Dict[str, Optional[str]] = first_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)'
    )
    self.assertEqual({
        'nestedFieldOne': (
            container_field_one_random_text_value if is_caching is not True else
            {'value': container_field_one_random_text_value, 'fromCache': False}
        ),
        'nestedFieldTwo': (
            container_field_two_random_text_value if is_caching is not True else
            {'value': container_field_two_random_text_value, 'fromCache': False}
        )
    }, first_table_retrieved_container_fields)

    second_table_retrieved_container_fields: Dict[str, Optional[int]] = second_table.remove_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)'
    )
    self.assertEqual({
        'nestedFieldOne': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        ),
        'nestedFieldTwo': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        )
    }, second_table_retrieved_container_fields)


def test_remove_multiple_fields(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    container_field_one_random_text_value: str = f"container_fieldOne_randomTextValue_{uuid4()}"
    container_field_two_random_text_value: str = f"container_fieldTwo_randomTextValue_{uuid4()}"
    first_table_container_fields_update_success: bool = first_table.update_multiple_fields(
        key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='container.nestedFieldOne', value_to_set=container_field_one_random_text_value),
            FieldSetter(field_path='container.nestedFieldTwo', value_to_set=container_field_two_random_text_value)
        ]
    )
    self.assertTrue(first_table_container_fields_update_success)
    if is_caching is True:
        self.assertTrue(first_table.commit_operations())
        first_table.clear_cached_data()

    first_table_retrieved_container_fields: Dict[str, Optional[str]] = first_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)'
    )
    self.assertEqual({
        'nestedFieldOne': (
            container_field_one_random_text_value if is_caching is not True else
            {'value': container_field_one_random_text_value, 'fromCache': False}
        ),
        'nestedFieldTwo': (
            container_field_two_random_text_value if is_caching is not True else
            {'value': container_field_two_random_text_value, 'fromCache': False}
        )
    }, first_table_retrieved_container_fields)

    second_table_retrieved_container_fields: Dict[str, Optional[int]] = second_table.remove_multiple_fields(
        key_value=TEST_ACCOUNT_ID, removers={
            'fieldOne': FieldRemover(field_path='container.nestedFieldOne'),
            'fieldTwo': FieldRemover(field_path='container.nestedFieldTwo')
        }
    )
    self.assertEqual({
        'fieldOne': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        ),
        'fieldTwo': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        )
    }, second_table_retrieved_container_fields)
