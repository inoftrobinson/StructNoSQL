import unittest
from typing import Optional, Union, Dict, Any, Tuple
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

    second_table_retrieved_simple_field_without_data_validation: Optional[Any] = second_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', data_validation=False
    )
    self.assertEqual((
        simple_field_random_text_value if is_caching is not True else
        {'value': simple_field_random_text_value, 'fromCache': False}
    ), second_table_retrieved_simple_field_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    second_table_retrieved_simple_field_with_data_validation: Optional[int] = second_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', data_validation=True
    )
    self.assertEqual((
        None if is_caching is not True else
        {'value': None, 'fromCache': False}
    ), second_table_retrieved_simple_field_with_data_validation)


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

    second_table_retrieved_container_fields_without_data_validation: Dict[str, Optional[Any]] = second_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)', data_validation=False
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
    }, second_table_retrieved_container_fields_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    second_table_retrieved_container_fields_with_data_validation: Dict[str, Optional[int]] = second_table.get_field(
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
    }, second_table_retrieved_container_fields_with_data_validation)


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

    second_table_retrieved_container_fields_without_data_validation: Dict[str, Optional[Any]] = second_table.get_multiple_fields(
        key_value=TEST_ACCOUNT_ID, data_validation=False, getters={
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
    }, second_table_retrieved_container_fields_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    second_table_retrieved_container_fields_with_data_validation: Dict[str, Optional[int]] = second_table.get_multiple_fields(
        key_value=TEST_ACCOUNT_ID, data_validation=True, getters={
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
    }, second_table_retrieved_container_fields_with_data_validation)


def test_remove_field(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    def generate_update_simple_field_text_value() -> str:
        simple_field_random_text_value: str = f"simpleField_randomTextValue_{uuid4()}"
        first_table_simple_field_update_success: bool = first_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path='simpleField', value_to_set=simple_field_random_text_value
        )
        self.assertTrue(first_table_simple_field_update_success)
        if is_caching is True:
            self.assertTrue(first_table.commit_operations())
            first_table.clear_cached_data()
        return simple_field_random_text_value

    first_generated_simple_field_random_text_value: str = generate_update_simple_field_text_value()
    second_table_removed_simple_field_without_data_validation: Optional[Any] = second_table.remove_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', data_validation=False
    )
    self.assertEqual((
        first_generated_simple_field_random_text_value if is_caching is not True else
        {'value': first_generated_simple_field_random_text_value, 'fromCache': False}
    ), second_table_removed_simple_field_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    second_generated_simple_field_random_text_value: str = generate_update_simple_field_text_value()
    second_table_removed_simple_field_with_data_validation: Optional[int] = second_table.remove_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', data_validation=True
    )
    self.assertEqual((
        None if is_caching is not True else
        {'value': None, 'fromCache': False}
    ), second_table_removed_simple_field_with_data_validation)


def test_remove_field_multi_selectors(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    def generate_update_container_fields_text_values() -> Tuple[str, str]:
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
        return container_field_one_random_text_value, container_field_two_random_text_value

    first_generated_container_field_one_random_text_value, first_generated_container_field_two_random_text_value = generate_update_container_fields_text_values()
    second_table_removed_container_fields_without_data_validation: Dict[str, Optional[Any]] = second_table.remove_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)', data_validation=False
    )
    self.assertEqual({
        'nestedFieldOne': (
            first_generated_container_field_one_random_text_value if is_caching is not True else
            {'value': first_generated_container_field_one_random_text_value, 'fromCache': False}
        ),
        'nestedFieldTwo': (
            first_generated_container_field_two_random_text_value if is_caching is not True else
            {'value': first_generated_container_field_two_random_text_value, 'fromCache': False}
        )
    }, second_table_removed_container_fields_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    second_generated_container_field_one_random_text_value, second_generated_container_field_two_random_text_value = generate_update_container_fields_text_values()
    second_table_removed_container_fields_with_data_validation: Dict[str, Optional[int]] = second_table.remove_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)', data_validation=True
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
    }, second_table_removed_container_fields_with_data_validation)


def test_remove_multiple_fields(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    def generate_update_container_fields_text_values() -> Tuple[str, str]:
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
        return container_field_one_random_text_value, container_field_two_random_text_value

    first_generated_container_field_one_random_text_value, first_generated_container_field_two_random_text_value = generate_update_container_fields_text_values()
    second_table_removed_container_fields_without_data_validation: Dict[str, Optional[Any]] = second_table.remove_multiple_fields(
        key_value=TEST_ACCOUNT_ID, data_validation=False, removers={
            'fieldOne': FieldRemover(field_path='container.nestedFieldOne'),
            'fieldTwo': FieldRemover(field_path='container.nestedFieldTwo')
        }
    )
    self.assertEqual({
        'fieldOne': (
            first_generated_container_field_one_random_text_value if is_caching is not True else
            {'value': first_generated_container_field_one_random_text_value, 'fromCache': False}
        ),
        'fieldTwo': (
            first_generated_container_field_two_random_text_value if is_caching is not True else
            {'value': first_generated_container_field_two_random_text_value, 'fromCache': False}
        )
    }, second_table_removed_container_fields_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    second_generated_container_field_one_random_text_value, second_generated_container_field_two_random_text_value = generate_update_container_fields_text_values()
    second_table_removed_container_fields_with_data_validation: Dict[str, Optional[int]] = second_table.remove_multiple_fields(
        key_value=TEST_ACCOUNT_ID, data_validation=True, removers={
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
    }, second_table_removed_container_fields_with_data_validation)


def test_grouped_remove_multiple_fields(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    def generate_update_container_fields_text_values() -> Tuple[str, str]:
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
        return container_field_one_random_text_value, container_field_two_random_text_value

    first_generated_container_field_one_random_text_value, first_generated_container_field_two_random_text_value = generate_update_container_fields_text_values()
    second_table_removed_container_fields_without_data_validation: Dict[str, Optional[Any]] = second_table.grouped_remove_multiple_fields(
        key_value=TEST_ACCOUNT_ID, data_validation=False, removers={
            'fieldOne': FieldRemover(field_path='container.nestedFieldOne'),
            'fieldTwo': FieldRemover(field_path='container.nestedFieldTwo')
        }
    )
    self.assertEqual({
        'fieldOne': (
            first_generated_container_field_one_random_text_value if is_caching is not True else
            {'value': first_generated_container_field_one_random_text_value, 'fromCache': False}
        ),
        'fieldTwo': (
            first_generated_container_field_two_random_text_value if is_caching is not True else
            {'value': first_generated_container_field_two_random_text_value, 'fromCache': False}
        )
    }, second_table_removed_container_fields_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    second_generated_container_field_one_random_text_value, second_generated_container_field_two_random_text_value = generate_update_container_fields_text_values()
    second_table_removed_container_fields_with_data_validation: Dict[str, Optional[int]] = second_table.grouped_remove_multiple_fields(
        key_value=TEST_ACCOUNT_ID, data_validation=True, removers={
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
    }, second_table_removed_container_fields_with_data_validation)


def test_update_field_return_old(
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

    second_table_retrieved_simple_field_without_data_validation: Optional[Any] = second_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', data_validation=False
    )
    self.assertEqual((
        simple_field_random_text_value if is_caching is not True else
        {'value': simple_field_random_text_value, 'fromCache': False}
    ), second_table_retrieved_simple_field_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    simple_field_dummy_int_value: int = 42
    second_table_simple_field_update_success, second_table_retrieved_old_simple_field_with_data_validation = second_table.update_field_return_old(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', value_to_set=simple_field_dummy_int_value
    )
    second_table_retrieved_old_simple_field: Optional[int]
    self.assertTrue(second_table_simple_field_update_success)
    self.assertEqual((
        None if is_caching is not True else
        {'value': None, 'fromCache': False}
    ), second_table_retrieved_old_simple_field_with_data_validation)


def test_update_multiple_fields_return_old(
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

    second_table_retrieved_container_fields_without_data_validation: Dict[str, Optional[Any]] = second_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)', data_validation=False
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
    }, second_table_retrieved_container_fields_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    second_table_container_fields_update_success, second_table_retrieved_container_fields_with_data_validation = second_table.update_multiple_fields_return_old(
        key_value=TEST_ACCOUNT_ID, data_validation=True, setters={
            'fieldOne': FieldSetter(field_path='container.nestedFieldOne', value_to_set=42),
            'fieldTwo': FieldSetter(field_path='container.nestedFieldTwo', value_to_set=42)
        }
    )
    second_table_retrieved_container_fields: Dict[str, Optional[int]]
    self.assertTrue(second_table_container_fields_update_success)
    self.assertEqual({
        'fieldOne': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        ),
        'fieldTwo': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        )
    }, second_table_retrieved_container_fields_with_data_validation)


def test_query_field(
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

    second_table_retrieved_simple_field_without_data_validation: Optional[Any] = second_table.query_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', data_validation=False
    )
    self.assertEqual({TEST_ACCOUNT_ID: (
        simple_field_random_text_value if is_caching is not True else
        {'value': simple_field_random_text_value, 'fromCache': False}
    )}, second_table_retrieved_simple_field_without_data_validation)
    
    if is_caching is True:
        second_table.clear_cached_data()

    second_table_retrieved_simple_field_with_data_validation: Optional[int] = second_table.query_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', data_validation=True
    )
    self.assertEqual({TEST_ACCOUNT_ID: (
        None if is_caching is not True else
        {'value': None, 'fromCache': False}
    )}, second_table_retrieved_simple_field_with_data_validation)


def test_query_field_multi_selectors(
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

    second_table_retrieved_container_fields_without_data_validation: Dict[str, Optional[Any]] = second_table.query_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)', data_validation=False
    )
    self.assertEqual({TEST_ACCOUNT_ID: {
        'nestedFieldOne': (
            container_field_one_random_text_value if is_caching is not True else
            {'value': container_field_one_random_text_value, 'fromCache': False}
        ),
        'nestedFieldTwo': (
            container_field_two_random_text_value if is_caching is not True else
            {'value': container_field_two_random_text_value, 'fromCache': False}
        )
    }}, second_table_retrieved_container_fields_without_data_validation)
    
    if is_caching is True:
        second_table.clear_cached_data()

    second_table_retrieved_container_fields_with_data_validation: Dict[str, Optional[int]] = second_table.query_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)', data_validation=True
    )
    self.assertEqual({TEST_ACCOUNT_ID: {
        'nestedFieldOne': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        ),
        'nestedFieldTwo': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        )
    }}, second_table_retrieved_container_fields_with_data_validation)


def test_query_multiple_fields(
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

    second_table_retrieved_container_fields_without_data_validation: Dict[str, Optional[Any]] = second_table.query_multiple_fields(
        key_value=TEST_ACCOUNT_ID, data_validation=False, getters={
            'one': FieldGetter(field_path='container.nestedFieldOne'),
            'two': FieldGetter(field_path='container.nestedFieldTwo'),
        }
    )
    self.assertEqual({TEST_ACCOUNT_ID: {
        'one': (
            container_field_one_random_text_value if is_caching is not True else
            {'value': container_field_one_random_text_value, 'fromCache': False}
        ),
        'two': (
            container_field_two_random_text_value if is_caching is not True else
            {'value': container_field_two_random_text_value, 'fromCache': False}
        )
    }}, second_table_retrieved_container_fields_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    second_table_retrieved_container_fields_with_data_validation: Dict[str, Optional[int]] = second_table.query_multiple_fields(
        key_value=TEST_ACCOUNT_ID, data_validation=True, getters={
            'one': FieldGetter(field_path='container.nestedFieldOne'),
            'two': FieldGetter(field_path='container.nestedFieldTwo'),
        }
    )
    self.assertEqual({TEST_ACCOUNT_ID: {
        'one': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        ),
        'two': (
            None if is_caching is not True else
            {'value': None, 'fromCache': False}
        )
    }}, second_table_retrieved_container_fields_with_data_validation)


def test_remove_record(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        is_caching: bool, primary_key_name: str
):
    random_record_id: str = f"recordId_{uuid4()}"
    simple_field_random_text_value: str = f"simpleField_randomTextValue_{uuid4()}"

    first_table_put_record_success: bool = first_table.put_record(
        record_dict_data={primary_key_name: random_record_id, 'simpleField': simple_field_random_text_value}
    )
    self.assertTrue(first_table_put_record_success)
    if is_caching is True:
        self.assertTrue(first_table.commit_operations())
        first_table.clear_cached_data()

    second_table_retrieved_simple_field_without_data_validation: Optional[Any] = second_table.get_field(
        key_value=random_record_id, field_path='simpleField', data_validation=False
    )
    self.assertEqual((
        simple_field_random_text_value if is_caching is not True else
        {'value': simple_field_random_text_value, 'fromCache': False}
    ), second_table_retrieved_simple_field_without_data_validation)

    if is_caching is True:
        second_table.clear_cached_data()

    second_table_removed_record_data_with_data_validation: Optional[dict] = second_table.remove_record(
        indexes_keys_selectors={primary_key_name: random_record_id}, data_validation=True
    )
    # The only valid field from the removed_record should be the primary_key field. Since the simpleField
    # will not be of a type matching the model, its key should not be included in the removed_record_data.
    self.assertEqual((
        {primary_key_name: random_record_id} if is_caching is not True else
        {'value': {primary_key_name: random_record_id}, 'fromCache': False}
    ), second_table_removed_record_data_with_data_validation)
