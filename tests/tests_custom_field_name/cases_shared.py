import unittest
from typing import Union, Optional
from uuid import uuid4

from StructNoSQL import DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineCachingTable, \
    InoftVocalEngineBasicTable, FieldTargetNotFoundException
from tests.components.playground_table_clients import TEST_ACCOUNT_ID


def test_set_update_field_with_custom_name(
        self: unittest.TestCase, users_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, InoftVocalEngineBasicTable, InoftVocalEngineCachingTable],
        primary_key_name: str, is_caching: bool
):
    simple_field_random_value = f"simpleField_{uuid4()}"
    simple_field_update_success: bool = users_table.update_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField', value_to_set=simple_field_random_value
    )
    self.assertTrue(simple_field_update_success)

    retrieved_simple_field_value: Optional[str] = users_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='simpleField'
    )
    self.assertEqual((
        simple_field_random_value if is_caching is not True else
        {'fromCache': True, 'value': simple_field_random_value}
    ), retrieved_simple_field_value)

    custom_name_field_random_value = f"customNameField_{uuid4()}"
    custom_name_field_update_success: bool = users_table.update_field(
        key_value=TEST_ACCOUNT_ID, field_path='field%/!$', value_to_set=custom_name_field_random_value
    )
    self.assertTrue(custom_name_field_update_success)

    retrieved_custom_name_field_value: Optional[str] = users_table.get_field(
        key_value=TEST_ACCOUNT_ID, field_path='field%/!$'
    )
    self.assertEqual((
        custom_name_field_random_value if is_caching is not True else
        {'fromCache': True, 'value': custom_name_field_random_value}
    ), retrieved_custom_name_field_value)

    try:
        users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldWithCustomName')
        self.fail()
    except FieldTargetNotFoundException as e:
        pass

if __name__ == '__main__':
    unittest.main()
