import random
import unittest
from typing import List, Optional, Dict
from uuid import uuid4

from StructNoSQL import FieldGetter, FieldSetter, FieldRemover, BaseField, MapModel, TableDataModel
from StructNoSQL.exceptions import FieldTargetNotFoundException
from tests.users_table import UsersTable, TEST_ACCOUNT_ID, TEST_PROJECT_ID, TEST_ACCOUNT_EMAIL, TEST_ACCOUNT_USERNAME


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    fieldToRemove = BaseField(name='fieldAlreadyInitializedRequiringNewValue', field_type=str, required=False)
    class ContainerModel(MapModel):
        firstNestedValue = BaseField(name='firstNestedValue', field_type=str, required=False)
    containerThatWillFailRequestAndRequireFieldsInitialization = BaseField(
        name='containerThatWillFailRequestAndRequireFieldsInitialization',
        field_type=Dict[str, ContainerModel], key_name='id', required=False
    )

class TestRemoveFieldOperations(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = UsersTable(data_model=TableModel())

    def test_request_fails_on_fields_initialization_but_require_existing_field_to_be_updated(self):
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


if __name__ == '__main__':
    unittest.main()
