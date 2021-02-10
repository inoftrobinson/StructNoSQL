import unittest
from typing import List, Optional, Dict
from uuid import uuid4

from StructNoSQL import FieldGetter, FieldSetter, FieldRemover, BaseField, MapModel, TableDataModel
from StructNoSQL.exceptions import FieldTargetNotFoundException
from tests.users_table import UsersTable, TEST_ACCOUNT_ID, TEST_PROJECT_ID, TEST_ACCOUNT_EMAIL, TEST_ACCOUNT_USERNAME


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    fieldToRemove = BaseField(name='fieldToRemove', field_type=str, required=False)
    class ContainerModel(MapModel):
        firstNestedValue = BaseField(name='firstNestedValue', field_type=str, required=False)
        secondNestedValue = BaseField(name='secondNestedValue', field_type=str, required=False)
    sophisticatedFieldToRemove = BaseField(name='sophisticatedFieldToRemove', field_type=Dict[str, ContainerModel], key_name='id', required=False)

class TestRemoveFieldOperations(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = UsersTable(data_model=TableModel())

    def test_delete_basic_item_from_path_target(self):
        success_field_set = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='fieldToRemove',
            value_to_set="yolo mon ami !"
        )
        self.assertTrue(success_field_set)

        success_field_remove = self.users_table.delete_field(
            key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove'
        )
        self.assertTrue(success_field_remove)

        retrieved_field_data = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove'
        )
        self.assertIsNone(retrieved_field_data)

    def test_remove_sophisticated_item_from_path_target(self):
        query_kwargs = {'id': 'sampleId'}

        first_message = "Soooooo, does it works ? :)"
        second_message = "Yes, setting a second field setter is inefficient, everything could be done by setting the map itself."

        def set_data():
            success_field_set = self.users_table.update_multiple_fields(
                key_value=TEST_ACCOUNT_ID, setters=[
                    FieldSetter(
                        field_path="sophisticatedFieldToRemove.{{id}}.firstNestedValue",
                        query_kwargs=query_kwargs, value_to_set=first_message
                    ),
                    FieldSetter(
                        field_path="sophisticatedFieldToRemove.{{id}}.secondNestedValue",
                        query_kwargs=query_kwargs, value_to_set=second_message
                    )
                ]
            )
            self.assertTrue(success_field_set)
        set_data()

        removed_first_message = self.users_table.remove_field(
            field_path="sophisticatedFieldToRemove.{{id}}.firstNestedValue",
            key_value=TEST_ACCOUNT_ID, query_kwargs=query_kwargs,
        )
        self.assertEqual(first_message, removed_first_message)

        removed_second_message = self.users_table.remove_field(
            field_path="sophisticatedFieldToRemove.{{id}}.secondNestedValue",
            key_value=TEST_ACCOUNT_ID, query_kwargs=query_kwargs
        )
        self.assertEqual(second_message, removed_second_message)

        set_data()
        # Since we removed the individuals items, before we can retrieve
        # the entire data, we need to re-set it in the database.
        removed_entire_item = self.users_table.remove_field(
            field_path="sophisticatedFieldToRemove.{{id}}",
            key_value=TEST_ACCOUNT_ID, query_kwargs=query_kwargs,
        )
        self.assertEqual(removed_entire_item, {'firstNestedValue': first_message, 'secondNestedValue': second_message})
        # Python does not care about the dict ordering when doing a dict comparison


if __name__ == '__main__':
    unittest.main()