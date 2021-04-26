import unittest
from typing import Optional, Dict, Set, List, Any
from uuid import uuid4

from StructNoSQL import BaseField, MapModel, TableDataModel
from tests.users_table import UsersTable, TEST_ACCOUNT_ID, TEST_PROJECT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    class ContainerModel(MapModel):
        untypedList = BaseField(name='untypedList', field_type=list, key_name='listIndex', required=False)
        typedList = BaseField(name='typedList', field_type=List[str], key_name='listIndex', required=False)
    container = BaseField(name='container', field_type=ContainerModel, required=False)

class TestListObjectType(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = UsersTable(data_model=TableModel())

    def test_untyped_list(self):
        random_list_values: List[Any] = list(str(uuid4()) for i in range(5))
        single_valid_list_item = random_list_values[2]
        random_list_values.append(42)
        # Add a value of different type, that is valid to the untypedList

        set_success = self.users_table.update_field(
            index_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.untypedList', value_to_set=random_list_values
        )
        self.assertTrue(set_success)

        retrieved_list_item: Optional[str or int] = self.users_table.get_field(
            index_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.untypedList.{{listIndex}}',
            query_kwargs={'listIndex': 2}
        )
        self.assertEqual(retrieved_list_item, single_valid_list_item)

        retrieved_entire_list: Optional[list] = self.users_table.get_field(
            index_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.untypedList',
            query_kwargs={'listIndex': 2}
        )
        self.assertEqual(retrieved_entire_list, random_list_values)

        deletion_success = self.users_table.delete_field(
            index_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.untypedList'
        )
        self.assertTrue(deletion_success)

    def test_typed_list(self):
        valid_random_list_values: List[str] = list(str(uuid4()) for i in range(5))
        single_valid_list_item = valid_random_list_values[2]
        random_list_values = valid_random_list_values.copy()
        random_list_values.append(42)
        # Add invalid value to the random_list_values

        set_success = self.users_table.update_field(
            index_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList', value_to_set=random_list_values
        )
        self.assertTrue(set_success)

        query_kwargs = {'listIndex': 2}
        retrieved_list_item: Optional[dict] = self.users_table.get_field(
            index_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList.{{listIndex}}', query_kwargs=query_kwargs
        )
        self.assertEqual(retrieved_list_item, single_valid_list_item)

        retrieved_entire_list: Optional[list] = self.users_table.get_field(
            index_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList', query_kwargs=query_kwargs
        )
        self.assertEqual(valid_random_list_values, retrieved_entire_list)

        deletion_success = self.users_table.delete_field(
            index_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList', query_kwargs=query_kwargs
        )
        self.assertTrue(deletion_success)


if __name__ == '__main__':
    unittest.main()
