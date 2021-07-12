import unittest
from typing import Dict
from uuid import uuid4

from StructNoSQL import FieldGetter, FieldSetter, BaseField, MapModel, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    fieldAlreadyInitializedRequiringNewValue = BaseField(field_type=str, required=False)
    class ContainerModel(MapModel):
        firstNestedValue = BaseField(field_type=str, required=False)
        class NestedContainerModel(MapModel):
            secondNestedValue = BaseField(field_type=str, required=False)
        nestedContainer = BaseField(field_type=Dict[str, NestedContainerModel], required=False)
    containerThatWillFailRequestAndRequireFieldsInitialization = BaseField(field_type=Dict[str, ContainerModel], key_name='id', required=False)

class TestRequestFailsOnFieldsInitializationButRequireExistingFieldsToBeUpdateD(unittest.TestCase):
    """
    This test a rare case, where if the update_multiple_fields is used, and one of the field to update caused the
    request to failed (for example, trying to navigate into an item that does not exist), the library will of course
    try to initialize the missing fields. The library has no idea which field caused the request to fail, so, it will
    try to initialize all of the field, one per one, only if they are not existing. In a single update operation,
    this never cause issues, but in a multi fields update operation, we could have an existing field, that did not
    cause the operation the fail, and that would need to be modify. Yet, since it already exist, the library will not
    correctly modify it as the request instructed it to do. To prevent this issue, if an item already exist the library
    will retrieve its UPDATED_NEW value in the same request (to avoid a get request), and check if the value present in
    the database is the value that our request specified. If its not the case, we will send a new request to modify the
    value. This request will not try to initialize fields if she fails, to avoid potential infinite recursion.
    """

    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

    def test_with_string_value_as_target(self):
        random_id = str(uuid4())
        random_value_one = f"one_{uuid4()}"
        random_value_two = f"two_{uuid4()}"

        initial_update_with_empty_value_success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path='fieldAlreadyInitializedRequiringNewValue', value_to_set="initial"
        )
        self.assertTrue(initial_update_with_empty_value_success)

        update_success = self.users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='fieldAlreadyInitializedRequiringNewValue', value_to_set=random_value_one),
            FieldSetter(
                field_path='containerThatWillFailRequestAndRequireFieldsInitialization.{{id}}.firstNestedValue',
                query_kwargs={'id': random_id}, value_to_set=random_value_two
            )
        ])
        self.assertTrue(update_success)

        get_response_data = self.users_table.get_multiple_fields(key_value=TEST_ACCOUNT_ID, getters={
            'one': FieldGetter(field_path='fieldAlreadyInitializedRequiringNewValue'),
            'two': FieldGetter(field_path='containerThatWillFailRequestAndRequireFieldsInitialization.{{id}}.firstNestedValue', query_kwargs={'id': random_id})
        })
        self.assertEqual(get_response_data.get('one', None), random_value_one)
        self.assertEqual(get_response_data.get('two', None), random_value_two)

    def test_with_dict_values_as_target(self):
        nested_container_field_path = 'containerThatWillFailRequestAndRequireFieldsInitialization.{{id}}.nestedContainer'
        random_first_container_id_one = f"first_container_{uuid4()}"
        random_first_container_id_two = f"second_container_{uuid4()}"
        random_nested_container_id_one = f"id_one_{str(uuid4())}"
        random_nested_container_id_two = f"id_two_{str(uuid4())}"
        random_container_field_value_one = {'secondNestedValue': f"container_field_value_one_{uuid4()}"}
        random_container_field_value_two = {'secondNestedValue': f"container_field_value_two_{uuid4()}"}

        # Initial update that put a single item in the container without risking to override existing fields
        initial_update_with_empty_value_success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path=nested_container_field_path + '.{{nestedContainerKey}}',
            query_kwargs={'id': random_first_container_id_one, 'nestedContainerKey': random_nested_container_id_one},
            value_to_set=random_container_field_value_one
        )
        self.assertTrue(initial_update_with_empty_value_success)

        # Multi update operation, that could cause the
        update_success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path=nested_container_field_path + '.{{nestedContainerKey}}',
            query_kwargs={'id': random_first_container_id_two, 'nestedContainerKey': random_nested_container_id_two},
            value_to_set=random_container_field_value_two
        )
        self.assertTrue(update_success)

        retrieved_containers_data = self.users_table.get_multiple_fields(key_value=TEST_ACCOUNT_ID, getters={
            'one': FieldGetter(field_path=nested_container_field_path, query_kwargs={'id': random_first_container_id_one}),
            'two': FieldGetter(field_path=nested_container_field_path, query_kwargs={'id': random_first_container_id_two})
        })
        self.assertEqual(retrieved_containers_data['one'].get(random_nested_container_id_one, None), random_container_field_value_one)
        self.assertEqual(retrieved_containers_data['two'].get(random_nested_container_id_two, None), random_container_field_value_two)


if __name__ == '__main__':
    unittest.main()
