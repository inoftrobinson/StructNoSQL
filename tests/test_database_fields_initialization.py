import random
import unittest
from typing import Optional, Dict

from StructNoSQL import BaseField, MapModel, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    class NestedObjectItem(MapModel):
        value = BaseField(field_type=int, required=False)
    nestedObject = BaseField(field_type=Dict[str, NestedObjectItem], key_name='nestedObjectId', required=False)


class TestDatabaseFieldsInitialization(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

    def test_initialize_new_nested_object(self):
        field_random_value: int = random.randint(0, 100)
        project_id = "testFieldInitializationNewProjectId"
        query_kwargs = {'nestedObjectId': project_id}

        set_success: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='nestedObject.{{nestedObjectId}}.value',
            query_kwargs=query_kwargs, value_to_set=field_random_value
        )
        self.assertTrue(set_success)

        retrieved_value: Optional[int] = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID, field_path='nestedObject.{{nestedObjectId}}.value', query_kwargs=query_kwargs
        )
        self.assertEqual(retrieved_value, field_random_value)

        deletion_success: bool = self.users_table.delete_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='nestedObject.{{nestedObjectId}}',
            query_kwargs=query_kwargs
        )
        self.assertTrue(deletion_success)


if __name__ == '__main__':
    unittest.main()
