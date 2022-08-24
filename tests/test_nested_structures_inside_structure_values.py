import unittest
from typing import Set, Optional, Dict, List
from uuid import uuid4

from StructNoSQL import BaseField, MapModel, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    nestedDictDictStructure = BaseField(field_type=Dict[str, Dict[str, bool]], required=False, key_name='itemKey')
    class NestedModelToIndex(MapModel):
        value = BaseField(field_type=str, required=False)
    nestedDictDictModelStructure = BaseField(field_type=Dict[str, Dict[str, NestedModelToIndex]], required=False, key_name='itemKey')
    # nestedDictListStructure = BaseField(field_type=Dict[str, List[str]], required=False)
    # nestedDictSetStructure = BaseField(field_type=Dict[str, Set[str]], required=False)


class TestsNestedStructuresInsideStructureValues(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

    def test_nested_dict_dict_structure(self):
        random_parent_key = f"parentKey_{uuid4()}"
        random_child_key = f"childKey_{uuid4()}"

        keys_fields_switch = list(self.users_table.fields_switch.keys())
        self.assertIn('nestedDictDictStructure.{{itemKey}}.{{itemKeyChild}}', keys_fields_switch)

        update_success: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='nestedDictDictStructure.{{itemKey}}.{{itemKeyChild}}',
            query_kwargs={'itemKey': random_parent_key, 'itemKeyChild': random_child_key},
            value_to_set=True
        )
        self.assertTrue(update_success)

        retrieved_item = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='nestedDictDictStructure.{{itemKey}}',
            query_kwargs={'itemKey': random_parent_key}
        )
        self.assertEqual(retrieved_item, {random_child_key: True})

        removed_item = self.users_table.remove_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='nestedDictDictStructure.{{itemKey}}',
            query_kwargs={'itemKey': random_parent_key}
        )
        self.assertEqual(removed_item, {random_child_key: True})

        retrieved_expected_none_item = self.users_table.get_field(
            TEST_ACCOUNT_ID,
            field_path='nestedDictDictStructure.{{itemKey}}',
            query_kwargs={'itemKey': random_parent_key}
        )
        self.assertIsNone(retrieved_expected_none_item)

    def test_nested_dict_list_structure(self):
        # todo: implement
        pass

    def test_nested_dict_set_structure(self):
        # todo: implement
        pass

    def test_nested_dict_dict_model_structure(self):
        random_parent_key = f"parentKey_{uuid4()}"
        random_child_key = f"childKey_{uuid4()}"
        random_nested_model_value = f"value_{uuid4()}"

        keys_fields_switch = list(self.users_table.fields_switch.keys())
        self.assertIn('nestedDictDictModelStructure.{{itemKey}}.{{itemKeyChild}}', keys_fields_switch)

        update_success: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='nestedDictDictModelStructure.{{itemKey}}.{{itemKeyChild}}',
            query_kwargs={'itemKey': random_parent_key, 'itemKeyChild': random_child_key},
            value_to_set={'value': random_nested_model_value}
        )
        self.assertTrue(update_success)

        retrieved_nested_model_value = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='nestedDictDictModelStructure.{{itemKey}}.{{itemKeyChild}}.value',
            query_kwargs={'itemKey': random_parent_key, 'itemKeyChild': random_child_key}
        )
        self.assertEqual(retrieved_nested_model_value, random_nested_model_value)

        removed_item = self.users_table.remove_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='nestedDictDictModelStructure.{{itemKey}}',
            query_kwargs={'itemKey': random_parent_key}
        )
        self.assertEqual(removed_item, {random_child_key: {'value': random_nested_model_value}})

        retrieved_expected_none_item = self.users_table.get_field(
            TEST_ACCOUNT_ID,
            field_path='nestedDictDictModelStructure.{{itemKey}}',
            query_kwargs={'itemKey': random_parent_key}
        )
        self.assertIsNone(retrieved_expected_none_item)


if __name__ == '__main__':
    unittest.main()
