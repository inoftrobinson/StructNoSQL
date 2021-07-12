import unittest
from typing import List, Optional, Dict
from uuid import uuid4

from StructNoSQL import FieldGetter, FieldSetter, FieldRemover, BaseField, MapModel, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    fieldToRemove = BaseField(field_type=str, required=False)
    class ContainerModel(MapModel):
        firstNestedValue = BaseField(field_type=str, required=False)
        secondNestedValue = BaseField(field_type=str, required=False)
        thirdNestedValue = BaseField(field_type=str, required=False)
    sophisticatedFieldToRemove = BaseField(field_type=Dict[str, ContainerModel], key_name='id', required=False)

class TestRemoveFieldOperations(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

    def test_delete_basic_item_from_path_target(self):
        random_value = f"fieldToRemove_{uuid4()}"

        success_field_set: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='fieldToRemove',
            value_to_set=random_value
        )
        self.assertTrue(success_field_set)

        retrieved_field: Dict[str, dict] = self.users_table.query_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='fieldToRemove',
        )
        self.assertEqual(retrieved_field, {TEST_ACCOUNT_ID: random_value})

        retrieved_fields: Dict[str, dict] = self.users_table.query_multiple_fields(
            key_value=TEST_ACCOUNT_ID, getters={
                'fieldToRemove': FieldGetter(field_path='fieldToRemove')
            }
        )
        self.assertEqual(retrieved_fields, {TEST_ACCOUNT_ID: {'fieldToRemove': random_value}})

        """success_field_remove = self.users_table.delete_field(
            key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove'
        )
        self.assertTrue(success_field_remove)

        retrieved_field_data = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove'
        )
        self.assertIsNone(retrieved_field_data)"""

    """def test_remove_sophisticated_item_from_path_target(self):
        query_kwargs = {'id': 'sampleId'}

        first_message = f"one_{uuid4()}"
        second_message = f"two_{uuid4()}"

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

    def test_remove_multiple_fields(self):
        random_id = str(uuid4())
        random_value_one = f"one_{uuid4()}"
        random_value_two = f"two_{uuid4()}"

        update_success = self.users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='fieldToRemove', value_to_set=random_value_one),
            FieldSetter(
                field_path='sophisticatedFieldToRemove.{{id}}.firstNestedValue',
                query_kwargs={'id': random_id}, value_to_set=random_value_two
            )
        ])
        self.assertTrue(update_success)

        get_response_data = self.users_table.get_multiple_fields(key_value=TEST_ACCOUNT_ID, getters={
            'one': FieldGetter(field_path='fieldToRemove'),
            'two': FieldGetter(field_path='sophisticatedFieldToRemove.{{id}}.firstNestedValue', query_kwargs={'id': random_id})
        })
        self.assertEqual(get_response_data.get('one', None), random_value_one)
        self.assertEqual(get_response_data.get('two', None), random_value_two)

        remove_response_data = self.users_table.remove_multiple_fields(key_value=TEST_ACCOUNT_ID, removers={
            'one': FieldRemover(field_path='fieldToRemove'),
            'two': FieldRemover(field_path='sophisticatedFieldToRemove.{{id}}.firstNestedValue', query_kwargs={'id': random_id})
        })
        print(remove_response_data)
    """

if __name__ == '__main__':
    unittest.main()
