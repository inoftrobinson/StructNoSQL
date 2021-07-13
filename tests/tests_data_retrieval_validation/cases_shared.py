import unittest
from typing import Optional, Union, Dict, Any
from uuid import uuid4

from StructNoSQL import DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineCachingTable, FieldSetter
from StructNoSQL.middlewares.inoft_vocal_engine.inoft_vocal_engine_basic_table import InoftVocalEngineBasicTable
from tests.components.playground_table_clients import TEST_ACCOUNT_ID


def test_get_field(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
):
    simple_field_random_text_value: str = f"simpleField_randomTextValue_{uuid4()}"
    first_table_simple_field_update_success: bool = first_table.update_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', value_to_set=simple_field_random_text_value
    )
    self.assertTrue(first_table_simple_field_update_success)

    second_table_retrieved_simple_field: Optional[int] = second_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField'
    )
    self.assertIsNone(second_table_retrieved_simple_field)

def test_get_field_multi_selectors(
        self: unittest.TestCase,
        first_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        second_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
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

    second_table_retrieved_simple_field: Dict[str, Optional[int]] = second_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='container.(nestedFieldOne, nestedFieldTwo)'
    )
    self.assertEqual({'nestedFieldOne': None, 'nestedFieldTwo': None}, second_table_retrieved_simple_field)



