import unittest
from typing import Optional, Dict
from uuid import uuid4

from StructNoSQL import BaseField, MapModel, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    class ContainerItem(MapModel):
        value = BaseField(field_type=int, required=False)
    container = BaseField(field_type=Dict[str, ContainerItem], key_name='itemId', required=False)


class TestFieldKeyWithSpecialChars(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

    def test_field_key_with_points(self):
        random_item_id = f".pointBefore.{str(uuid4())}.pointAfter."
        query_kwargs = {'itemId': random_item_id}

        set_success: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path='container.{{itemId}}',
            query_kwargs=query_kwargs, value_to_set={'value': 42}
        )
        self.assertTrue(set_success)

        retrieved_value = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='container.{{itemId}}.value',
            query_kwargs=query_kwargs
        )
        self.assertEqual(retrieved_value, 42)

        deletion_success: bool = self.users_table.delete_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='container.{{itemId}}',
            query_kwargs=query_kwargs
        )
        self.assertTrue(deletion_success)


if __name__ == '__main__':
    unittest.main()
