import unittest
from typing import Optional, Dict

from StructNoSQL import BaseField, MapModel
from tests.users_table import UsersTable, TEST_ACCOUNT_ID, TEST_PROJECT_ID, UsersTableModel


class TableModel(UsersTableModel):
    class NestedObjectItem(MapModel):
        value = BaseField(name='value', field_type=int, required=False)
    nestedObject = BaseField(name='nestedObject', field_type=Dict[str, NestedObjectItem], key_name='nestedObjectId', required=False)


class TestDatabaseFieldsInitialization(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = UsersTable(data_model=TableModel())

    def test_initialize_new_nested_object(self):
        project_id = "testFieldInitializationNewProjectId"
        query_kwargs = {'nestedObjectId': project_id}

        set_success = self.users_table.set_update_one_field_value_in_single_record(
            key_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='nestedObject.{{nestedObjectId}}.value',
            query_kwargs=query_kwargs, value_to_set=42
        )
        self.assertTrue(set_success)

        retrieved_value = self.users_table.get_one_field_value_from_single_record(
            key_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='nestedObject.{{nestedObjectId}}.value',
            query_kwargs=query_kwargs
        )
        self.assertEqual(retrieved_value, 42)

        deletion_success = self.users_table.remove_one_field_item_in_single_record(
            key_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='nestedObject.{{nestedObjectId}}',
            query_kwargs=query_kwargs
        )
        self.assertTrue(deletion_success)


if __name__ == '__main__':
    unittest.main()
