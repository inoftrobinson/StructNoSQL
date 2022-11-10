import random
import unittest
from uuid import uuid4
from typing import Dict, Any, Union, List, Optional, Generator, Tuple

from StructNoSQL import FieldSetter, DynamoDBCachingTable, DynamoDBBasicTable, QueryMetadata
from tests.components.playground_table_clients import TEST_ACCOUNT_ID, TEST_ACCOUNT_USERNAME


def test_set_get_fields_with_primary_index(
        self: unittest.TestCase, users_table: Union[DynamoDBBasicTable, DynamoDBCachingTable],
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
    self.assertEqual({
        'fieldOne': (
            field1_random_value if is_caching is not True else
            {'fromCache': True, 'value': field1_random_value}
        ),
        'fieldTwo': (
            field2_random_value if is_caching is not True else
            {'fromCache': True, 'value': field2_random_value}
        )
    }, retrieved_values)

    single_field_not_primary_key, query_metadata = users_table.query_field(key_value=TEST_ACCOUNT_ID, field_path='fieldOne')
    self.assertEqual({TEST_ACCOUNT_ID: (
        field1_random_value if is_caching is not True else
        {'fromCache': True, 'value': field1_random_value}
    )}, single_field_not_primary_key)

    single_field_primary_key, query_metadata = users_table.query_field(key_value=TEST_ACCOUNT_ID, field_path=f'{primary_key_name}')
    self.assertEqual({TEST_ACCOUNT_ID: (
        TEST_ACCOUNT_ID if is_caching is not True else
        {'fromCache': False, 'value': TEST_ACCOUNT_ID}
    )}, single_field_primary_key)

    multiple_fields_without_primary_key, query_metadata = users_table.query_field(
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
        }
    }, multiple_fields_without_primary_key)

    multiple_fields_with_primary_key, query_metadata = users_table.query_field(
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
        }
    }, multiple_fields_with_primary_key)


def test_set_get_paginated_fields_with_primary_index(
        self: unittest.TestCase, users_table: Union[DynamoDBBasicTable, DynamoDBCachingTable],
        primary_key_name: str, is_caching: bool
):
    random_type_id: str = f"type_{uuid4()}"

    def make_record() -> str:
        for i_attempt in range(10):
            new_record_id: str = str(uuid4())
            existing_primary_key_value: Optional[Any] = users_table.get_field(
                key_value=new_record_id, field_path=primary_key_name
            )
            no_existing_record_found_for_new_record_id: bool = (
                existing_primary_key_value is None if is_caching is not True else
                existing_primary_key_value['value'] is None
            )
            if no_existing_record_found_for_new_record_id is True:
                put_record_success: bool = users_table.put_record({
                    primary_key_name: new_record_id,
                    'username': 'dummyUsername',
                    'email': 'dummyEmail',
                    'type': random_type_id
                })
                self.assertTrue(put_record_success)
                return new_record_id
        self.fail(msg="Could not generate unique record id after ten attempts")

    created_account_ids: List[str] = [make_record() for _ in range(5)]

    records_paginator: Generator[Tuple[Optional[dict], QueryMetadata], None, None] = users_table.paginated_query_field(
        index_name='type', key_value=random_type_id,
        field_path='accountId', pagination_records_limit=2
    )
    for records_data, query_metadata in records_paginator:
        if records_data is not None:
            for record_item_data in records_data:
                record_deletion_success: bool = users_table.delete_record(indexes_keys_selectors={
                    primary_key_name: record_item_data
                })
                self.assertTrue(record_deletion_success)
