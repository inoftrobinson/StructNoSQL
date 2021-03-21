import unittest
from uuid import uuid4

from StructNoSQL import BaseField, MapModel, TableDataModel, FieldSetter, MapField
from tests.users_table import UsersTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    class FieldsToDeleteContainerModel(MapModel):
        existingItemOneToDelete = BaseField(name='existingItemOneToDelete', field_type=str, required=False)
        existingItemTwoToDelete = BaseField(name='existingItemTwoToDelete', field_type=str, required=False)
    fieldsToDeleteContainer = MapField(name='fieldsToDeleteContainer', model=FieldsToDeleteContainerModel)

class TestDeleteNonExistingItems(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = UsersTable(data_model=TableModel())

    def test_delete_nested(self):
        success_delete_item_one = self.users_table.delete_field(key_value=TEST_ACCOUNT_ID, field_path='fieldsToDeleteContainer.existingItemOneToDelete')
        success_delete_item_two = self.users_table.delete_field(key_value=TEST_ACCOUNT_ID, field_path='fieldsToDeleteContainer.existingItemOneToDelete')
        self.assertTrue(success_delete_item_one)
        self.assertTrue(success_delete_item_two)

        random_item_one_value = f"itemOne_{uuid4()}"
        random_item_two_value = f"itemTwo_{uuid4()}"

        update_success = self.users_table.update_multiple_fields(
            key_value=TEST_ACCOUNT_ID, setters=[
                FieldSetter(field_path='fieldsToDeleteContainer.existingItemOneToDelete', value_to_set=random_item_one_value),
                FieldSetter(field_path='fieldsToDeleteContainer.existingItemOneToDelete', value_to_set=random_item_two_value)
            ]
        )
        self.assertTrue(update_success)


if __name__ == '__main__':
    unittest.main()
