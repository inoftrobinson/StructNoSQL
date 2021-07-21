import unittest
from typing import Optional, Union
from uuid import uuid4

from StructNoSQL import DynamoDBBasicTable, DynamoDBCachingTable, ExternalDynamoDBApiCachingTable
from StructNoSQL.clients_middlewares.external_dynamodb_api.external_dynamodb_api_basic_table import ExternalDynamoDBApiBasicTable


def test_basic_record_removal(
        self: unittest.TestCase, users_table: Union[DynamoDBBasicTable, DynamoDBCachingTable, ExternalDynamoDBApiBasicTable, ExternalDynamoDBApiCachingTable],
        primary_key_name: str, is_caching: bool
):
    random_record_id: str = f"recordId_{uuid4()}"
    random_value: str = f"value_{uuid4()}"
    put_record_success: bool = users_table.put_record(record_dict_data={
        primary_key_name: random_record_id, 'value': random_value
    })
    self.assertTrue(put_record_success)

    removed_record_data: Optional[dict] = users_table.remove_record(indexes_keys_selectors={primary_key_name: random_record_id})
    # todo: simplify the remove_record and delete_record, since deletion can only be done with the primary index,
    #  replace the (indexes_keys_selectors: dict) argument by a simple (primary_key_value: str)
    self.assertEqual((
        {primary_key_name: random_record_id, 'value': random_value} if is_caching is not True else
        {'value': {primary_key_name: random_record_id, 'value': random_value}, 'fromCache': False}
    ), removed_record_data)
