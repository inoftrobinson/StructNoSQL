import unittest
from typing import Optional, Dict, Set, List, Any
from uuid import uuid4

from StructNoSQL import BaseField, MapModel, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID


class BasicTableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    class ContainerModel(MapModel):
        untypedList = BaseField(field_type=list, key_name='listIndex', required=False)
        typedList = BaseField(field_type=List[str], key_name='listIndex', required=False)
    container = BaseField(field_type=ContainerModel, required=False)

class TestListObjectType(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)

    def test_untyped_list(self):
        table_client = PlaygroundDynamoDBBasicTable(data_model=BasicTableModel)

        random_list_values: List[Any] = list(str(uuid4()) for i in range(5))
        single_valid_list_item = random_list_values[2]
        random_list_values.append(42)
        # Add a value of different type, that is valid to the untypedList

        set_success: bool = table_client.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='container.untypedList',
            value_to_set=random_list_values
        )
        self.assertTrue(set_success)

        retrieved_list_item: Optional[str or int] = table_client.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='container.untypedList.{{listIndex}}',
            query_kwargs={'listIndex': 2}
        )
        self.assertEqual(retrieved_list_item, single_valid_list_item)

        retrieved_entire_list: Optional[list] = table_client.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='container.untypedList',
            query_kwargs={'listIndex': 2}
        )
        self.assertEqual(retrieved_entire_list, random_list_values)

        deletion_success: bool = table_client.delete_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='container.untypedList'
        )
        self.assertTrue(deletion_success)

    def test_typed_list(self):
        table_client = PlaygroundDynamoDBBasicTable(data_model=BasicTableModel)

        valid_random_list_values: List[str] = [str(uuid4()) for i in range(5)]
        single_valid_list_item = valid_random_list_values[2]
        random_list_values = valid_random_list_values.copy()
        random_list_values.append(42)
        # Add invalid value to the random_list_values

        set_success: bool = table_client.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList',
            value_to_set=random_list_values
        )
        self.assertTrue(set_success)

        query_kwargs = {'listIndex': 2}
        retrieved_list_item: Optional[dict] = table_client.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList.{{listIndex}}',
            query_kwargs=query_kwargs
        )
        self.assertEqual(retrieved_list_item, single_valid_list_item)

        retrieved_entire_list: Optional[list] = table_client.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList',
            query_kwargs=query_kwargs
        )
        self.assertEqual(valid_random_list_values, retrieved_entire_list)

        deletion_success: bool = table_client.delete_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='container.typedList',
            query_kwargs=query_kwargs
        )
        self.assertTrue(deletion_success)

    def test_untyped_list_without_defined_key_name(self):
        class LocalBasicTableModel(TableDataModel):
            accountId = BaseField(field_type=str, required=True)
            untypedListWithoutDefinedKeyName = BaseField(field_type=list, required=False)
        try:
            table_client = PlaygroundDynamoDBBasicTable(data_model=LocalBasicTableModel)
            try:
                list_without_defined_key_name_field = table_client.fields_switch['untypedListWithoutDefinedKeyName']
                self.assertEqual(list_without_defined_key_name_field.key_name, "untypedListWithoutDefinedKeyNameIndex")
            except KeyError as e:
                self.fail("untypedListWithoutDefinedKeyName field not found in fields_switch")
        except TypeError as e:
            self.fail("key_name default value seems to not be defined")

    def test_typed_list_without_defined_key_name(self):
        class LocalBasicTableModel(TableDataModel):
            accountId = BaseField(field_type=str, required=True)
            typedListWithoutDefinedKeyName = BaseField(field_type=List[str], required=False)
        try:
            table_client = PlaygroundDynamoDBBasicTable(data_model=LocalBasicTableModel)
            try:
                list_without_defined_key_name_field = table_client.fields_switch['typedListWithoutDefinedKeyName']
                self.assertEqual(list_without_defined_key_name_field.key_name, "typedListWithoutDefinedKeyNameIndex")
            except KeyError as e:
                self.fail("typedListWithoutDefinedKeyName field not found in fields_switch")
        except TypeError as e:
            self.fail("key_name default value seems to not be defined")


if __name__ == '__main__':
    unittest.main()
