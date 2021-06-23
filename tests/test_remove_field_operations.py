import unittest
from typing import List, Optional, Dict
from uuid import uuid4

from StructNoSQL import FieldGetter, FieldSetter, FieldRemover, BaseField, MapModel, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    fieldToRemove = BaseField(name='fieldToRemove', field_type=str, required=False)
    class ContainerModel(MapModel):
        firstNestedValue = BaseField(name='firstNestedValue', field_type=str, required=False)
        secondNestedValue = BaseField(name='secondNestedValue', field_type=str, required=False)
        thirdNestedValue = BaseField(name='thirdNestedValue', field_type=str, required=False)
    sophisticatedFieldToRemove = BaseField(name='sophisticatedFieldToRemove', field_type=Dict[str, ContainerModel], key_name='id', required=False)

class TestRemoveFieldOperations(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

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

    def test_remove_multiple_fields_with_multi_selectors(self):
        """
        Removing multiple fields with a multi selector in single field_path in a remove_multiple_fields operation is
        awkward, because a single key per field_path can be assigned. Which means that the multiple fields retrieved
        with the multi selector must be send back in the same dictionary, like if it was a get_field with multi selectors.
        """

        random_id = str(uuid4())
        field_one_random = f"field_one_{uuid4()}"
        field_two_random = f"field_two_{uuid4()}"
        field_three_random = f"field_three_{uuid4()}"

        update_success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path='sophisticatedFieldToRemove.{{id}}',
            query_kwargs={'id': random_id}, value_to_set={
                'firstNestedValue': field_one_random,
                'secondNestedValue': field_two_random,
                'thirdNestedValue': field_three_random
            }
        )
        self.assertTrue(update_success)

        get_response_data = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID, query_kwargs={'id': random_id},
            field_path='sophisticatedFieldToRemove.{{id}}.(firstNestedValue, secondNestedValue, thirdNestedValue)'
        )
        self.assertEqual(get_response_data.get('firstNestedValue', None), field_one_random)
        self.assertEqual(get_response_data.get('secondNestedValue', None), field_two_random)
        self.assertEqual(get_response_data.get('thirdNestedValue', None), field_three_random)

        remove_response_data = self.users_table.remove_multiple_fields(key_value=TEST_ACCOUNT_ID, removers={
            'one': FieldRemover(field_path='sophisticatedFieldToRemove.{{id}}.(firstNestedValue, secondNestedValue)', query_kwargs={'id': random_id})
        })
        self.assertEqual(remove_response_data.get('one', {}).get('firstNestedValue', None), field_one_random)
        self.assertEqual(remove_response_data.get('one', {}).get('secondNestedValue', None), field_two_random)

        retrieved_third_value = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID, query_kwargs={'id': random_id},
            field_path='sophisticatedFieldToRemove.{{id}}.thirdNestedValue'
        )
        self.assertEqual(retrieved_third_value, field_three_random)


if __name__ == '__main__':
    unittest.main()
