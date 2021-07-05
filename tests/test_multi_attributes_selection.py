import unittest
from time import time
from typing import Optional, Dict
from uuid import uuid4

from StructNoSQL import BaseField, MapModel, FieldGetter, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    class ItemWithMultipleAttributes(MapModel):
        name = BaseField(name='name', field_type=str, required=False)
        value = BaseField(name='value', field_type=str, required=False)
        timestamp = BaseField(name='timestamp', field_type=(int, float), required=False)
    multiAttributesContainer = BaseField(name='multiAttributesContainer', field_type=Dict[str, ItemWithMultipleAttributes], key_name='itemKey', required=False)


class TestDatabaseFieldsInitialization(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

    def test_retrieve_multiple_attributes_from_single_object(self):
        """
        Test both the get_field and the get_multiple_fields function
        """

        randomized_item_key = f"key_{uuid4()}"
        random_name = f"name-{uuid4()}"
        random_value = f"value-{uuid4()}"
        timestamp = time()
        randomized_item = {
            'name': random_name,
            'value': random_value,
            'timestamp': timestamp
        }
        query_kwargs = {'itemKey': randomized_item_key}

        set_success: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='multiAttributesContainer.{{itemKey}}',
            query_kwargs=query_kwargs, value_to_set=randomized_item
        )
        self.assertTrue(set_success)

        retrieved_values: dict = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='multiAttributesContainer.{{itemKey}}.(name, value)',
            query_kwargs=query_kwargs
        )
        self.assertEqual(retrieved_values.get('name'), random_name)
        self.assertEqual(retrieved_values.get('value'), random_value)

        retrieved_values: dict = self.users_table.get_multiple_fields(key_value=TEST_ACCOUNT_ID, getters={
            'one': FieldGetter(field_path='multiAttributesContainer.{{itemKey}}.(name, value)', query_kwargs=query_kwargs)
        })
        self.assertEqual(retrieved_values['one'].get('name', None), random_name)
        self.assertEqual(retrieved_values['one'].get('value', None), random_value)

        deletion_success: bool = self.users_table.delete_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='multiAttributesContainer.{{itemKey}}',
            query_kwargs=query_kwargs
        )
        self.assertTrue(deletion_success)


if __name__ == '__main__':
    unittest.main()
