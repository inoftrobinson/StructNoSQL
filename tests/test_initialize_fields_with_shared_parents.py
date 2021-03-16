import unittest
from typing import Dict

from StructNoSQL import TableDataModel, BaseField, MapModel, FieldSetter, FieldRemover, FieldGetter
from tests.users_table import UsersTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    class ItemModel(MapModel):
        itemName = BaseField(name='itemName', field_type=str, required=False)
    containerForItemsWithSharedParents = BaseField(name='containerForItemsWithSharedParents', field_type=Dict[str, ItemModel], key_name='itemId', required=False)


class InitializeFieldsWithSharedParents(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = UsersTable(data_model=TableModel())

    def test_initialize_two_items_with_share_parents(self):
        base_field_path = 'containerForItemsWithSharedParents.{{itemId}}'
        item_name_field_path = f'{base_field_path}.itemName'

        potential_old_data_deletion_success = self.users_table.delete_multiple_fields(key_value=TEST_ACCOUNT_ID, removers=[
            FieldRemover(field_path=base_field_path, query_kwargs={'itemId': 'itemOne'}),
            FieldRemover(field_path=base_field_path, query_kwargs={'itemId': 'itemTwo'})
        ])
        # We do not check the success of the deletion operation, because at the time of writing that, if a delete operation try to delete
        # a field path that does not exist, the operation will crash and return a success of False, where what we want is just to make sure
        # the data is fully clean so we can make sure we initialize the items from scratch. We do not really care about removing the data.

        update_success = self.users_table.update_multiple_fields(
            key_value=TEST_ACCOUNT_ID, setters=[
                FieldSetter(field_path=item_name_field_path, query_kwargs={'itemId': 'itemOne'}, value_to_set="NameItemOne"),
                FieldSetter(field_path=item_name_field_path, query_kwargs={'itemId': 'itemTwo'}, value_to_set="NameItemTwo"),
            ]
        )
        self.assertTrue(update_success)

        retrieved_items_data = self.users_table.get_multiple_fields(key_value=TEST_ACCOUNT_ID, getters={
            'one': FieldGetter(field_path=base_field_path, query_kwargs={'itemId': 'itemOne'}),
            'two': FieldGetter(field_path=base_field_path, query_kwargs={'itemId': 'itemTwo'}),
        })
        self.assertIsNotNone(retrieved_items_data)
        self.assertEqual(retrieved_items_data.get('one', None), {'itemName': "NameItemOne"})
        self.assertEqual(retrieved_items_data.get('two', None), {'itemName': "NameItemTwo"})


if __name__ == '__main__':
    unittest.main()
