import unittest
from typing import Optional, Dict, Set, List
from uuid import uuid4

from StructNoSQL import BaseField, MapModel, TableDataModel, MapField
from tests.users_table import UsersTable, TEST_ACCOUNT_ID, TEST_PROJECT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    class ContainerModel(MapModel):
        unknownList = BaseField(name='unknownList', field_type=set, key_name='listIndex', required=False)
        typedList = BaseField(name='typedList', field_type=List[str], key_name='listIndex', required=False)
    container = MapField(name='container', model=ContainerModel)

class TestListObjectType(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = UsersTable(data_model=TableModel())

    def test_set_typed_list(self):
        random_set_values: List[str] = list(str(uuid4()) for i in range(5))
        single_valid_set_item = random_set_values[2]
        random_set_values.append(42)

        set_success = self.users_table.set_update_one_field_value_in_single_record(
            key_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList', value_to_set=random_set_values
        )
        self.assertTrue(set_success)

        query_kwargs = {'listIndex': 2}
        retrieved_value = self.users_table.get_one_field_value_from_single_record(
            key_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList.{{listIndex}}', query_kwargs=query_kwargs
        )
        self.assertEqual(retrieved_value, single_valid_set_item)

        deletion_success = self.users_table.remove_one_field_item_in_single_record(
            key_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList', query_kwargs=query_kwargs
        )
        self.assertTrue(deletion_success)


if __name__ == '__main__':
    unittest.main()
