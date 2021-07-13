import unittest
from typing import List, Optional
from uuid import uuid4

from StructNoSQL import FieldGetter, FieldSetter, FieldRemover
from StructNoSQL.exceptions import FieldTargetNotFoundException
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID, TEST_PROJECT_ID, TEST_ACCOUNT_EMAIL, TEST_ACCOUNT_USERNAME
from tests.users_table_model import UsersTableModel


class TestTableOperations(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=UsersTableModel)

    def test_get_name_of_one_project(self):
        response_data: Optional[str] = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='projects.{{projectId}}.projectName',
            query_kwargs={'projectId': TEST_PROJECT_ID}
        )
        self.assertIn("test", response_data)

    def test_update_project_name(self):
        success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='projects.{{projectId}}.projectName',
            query_kwargs={'projectId': TEST_PROJECT_ID},
            value_to_set="test2"
        )
        self.assertTrue(success)

        response_data: Optional[str] = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='projects.{{projectId}}.projectName',
            query_kwargs={'projectId': TEST_PROJECT_ID}
        )
        self.assertEqual(response_data, "test2")

    def test_update_entire_project_model(self):
        success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='projects.{{projectId}}',
            value_to_set={'projectName': "test3"},
            query_kwargs={'projectId': TEST_PROJECT_ID}
        )

        # todo: allow to set the item of a dict (currently, when doing a query on the projects object,
        #  we will perform an operation of the project map, and not on an individual project item).
        self.assertTrue(success)

        response_data: Optional[str] = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='projects.{{projectId}}.projectName',
            query_kwargs={'projectId': TEST_PROJECT_ID}
        )
        self.assertEqual(response_data, "test3")

    def test_update_entire_project_model_with_missing_project_name(self):
        success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='projects.{{projectId}}',
            value_to_set={},
            query_kwargs={'projectId': TEST_PROJECT_ID}
        )
        self.assertFalse(success)

    def test_update_entire_project_model_with_invalid_data(self):
        success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='projects.{{projectId}}',
            value_to_set={'invalidProjectName': "test4"},
            query_kwargs={'projectId': TEST_PROJECT_ID}
        )
        self.assertFalse(success)

        try:
            response_data: Optional[str] = self.users_table.get_field(
                key_value=TEST_ACCOUNT_ID,
                field_path='projects.{{projectId}}.invalidProjectName',
                query_kwargs={'projectId': TEST_PROJECT_ID}
            )
            # If we do not get an error while trying to access an invalid field in the
            # get_field function, then we failed the test.
            self.fail()
        except FieldTargetNotFoundException as e:
            print(e)

    def test_multi_types_field(self):
        success_str: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path="multiTypes",
            value_to_set="yolo"
        )
        self.assertTrue(success_str)

        success_none: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path="multiTypes",
            value_to_set=None
        )
        self.assertTrue(success_none)

        success_bool: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path="multiTypes",
            value_to_set=True
        )
        self.assertFalse(success_bool)

    def test_basic_get_multiple_fields_values_in_single_query(self):
        response_data: Optional[dict] = self.users_table.get_multiple_fields(
            key_value=TEST_ACCOUNT_ID, getters={
                'theAccountId': FieldGetter(field_path='accountId'),
                'theProjects': FieldGetter(field_path='projects')
            }
        )
        self.assertIsNotNone(response_data)
        self.assertEqual(response_data.get('theAccountId', None), TEST_ACCOUNT_ID)

    def test_sophisticated_get_multiple_fields_in_single_query(self):
        response_data: Optional[dict] = self.users_table.get_multiple_fields(
            key_value=TEST_ACCOUNT_ID, getters={
                'theAccountId': FieldGetter(field_path='accountId'),
                'theProjectName': FieldGetter(
                    field_path='projects.{{projectId}}.projectName',
                    query_kwargs={'projectId': TEST_PROJECT_ID}
                )
            }
        )
        self.assertIsNotNone(response_data)
        self.assertEqual(response_data.get('theAccountId', None), TEST_ACCOUNT_ID)
        self.assertIn("test", response_data.get('theProjectName', ""))

    def test_basic_update_multiple_fields_in_single_query(self):
        success: bool = self.users_table.update_multiple_fields(
            key_value=TEST_ACCOUNT_ID,  setters=[
                FieldSetter(field_path='number1', value_to_set=42),
                FieldSetter(field_path='string1', value_to_set="Quarante-deux")
            ]
        )
        self.assertTrue(success)

    def test_sophisticated_update_multiple_fields_in_single_query(self):
        success: bool = self.users_table.update_multiple_fields(
            key_value=TEST_ACCOUNT_ID, setters=[
                FieldSetter(field_path='number1', value_to_set=42),
                FieldSetter(
                    field_path='projects.{{projectId}}.projectName',
                    query_kwargs={'projectId': TEST_PROJECT_ID},
                    value_to_set="test5",
                )
            ]
        )
        self.assertTrue(success)

        project_name_data: Optional[str] = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='projects.{{projectId}}.projectName',
            query_kwargs={'projectId': TEST_PROJECT_ID},
        )
        self.assertEqual(project_name_data, "test5")

    def test_set_and_get_float_in_field_value(self):
        source_float_value = 10.42021023492

        set_float_value_success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='floatTest',
            value_to_set=source_float_value
        )
        self.assertTrue(set_float_value_success)

        retrieved_float_value: Optional[float] = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='floatTest'
        )
        self.assertEqual(retrieved_float_value, source_float_value)

    def test_remove_basic_item_from_path_target(self):
        success_field_set: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path="fieldToDelete",
            value_to_set="yolo mon ami !"
        )
        self.assertTrue(success_field_set)

        success_field_remove: bool = self.users_table.delete_field(
            key_value=TEST_ACCOUNT_ID,
            field_path="fieldToDelete"
        )
        self.assertTrue(success_field_remove)

        retrieved_field_data: Optional[float] = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path="fieldToDelete"
        )
        self.assertIsNone(retrieved_field_data)

    def test_remove_sophisticated_item_from_path_target(self):
        query_kwargs = {"id": "sampleId"}

        success_field_set = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='sophisticatedRemoval.{{id}}.nestedVariable',
            query_kwargs=query_kwargs,
            value_to_set="Soooooo, does it works ? :)"
        )
        self.assertTrue(success_field_set)

        success_field_remove = self.users_table.delete_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='sophisticatedRemoval.{{id}}.nestedVariable',
            query_kwargs=query_kwargs
        )
        self.assertTrue(success_field_remove)

        retrieved_field_data = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='sophisticatedRemoval.{{id}}.nestedVariable',
            query_kwargs=query_kwargs
        )
        self.assertIsNone(retrieved_field_data)

    def test_remove_multiple_basic_and_sophisticated_items_from_path_target(self):
        query_kwargs = {"id": "sampleIdTwo"}

        success_fields_set: bool = self.users_table.update_multiple_fields(
            key_value=TEST_ACCOUNT_ID, setters=[
                FieldSetter(
                    field_path='fieldToDelete',
                    value_to_set="multipleBasicAndSophisticatedRemoval"
                ),
                FieldSetter(
                    field_path='sophisticatedRemoval.{{id}}.nestedVariable',
                    query_kwargs=query_kwargs, value_to_set="nestedDude"
                )
            ]
        )
        self.assertTrue(success_fields_set)

        deletion_response = self.users_table.delete_multiple_fields(
            key_value=TEST_ACCOUNT_ID, removers={
                'field': FieldRemover(field_path="fieldToDelete"),
                'nested': FieldRemover(field_path="sophisticatedRemoval.{{id}}.nestedVariable", query_kwargs=query_kwargs)
            }
        )
        self.assertTrue(all(deletion_response.values()))

    def test_put_and_delete_records(self):
        success_put_invalid_record = self.users_table.put_record(record_dict_data={"invalidAccountId": "testAccountId", "multiTypes": "testPutRecord"})
        self.assertFalse(success_put_invalid_record)

        success_put_valid_record = self.users_table.put_record(record_dict_data={"accountId": "testAccountId", "multiTypes": "testPutRecord"})
        self.assertTrue(success_put_valid_record)

        success_delete_record_with_index_typo = self.users_table.delete_record(indexes_keys_selectors={"accountIdWithTypo": "testAccountId"})
        self.assertFalse(success_delete_record_with_index_typo)

        success_delete_record_without_typo = self.users_table.delete_record(indexes_keys_selectors={"accountId": "testAccountId"})
        self.assertTrue(success_delete_record_without_typo)

    def test_get_value_from_path_target_by_secondary_index(self):
        account_id: Optional[str] = self.users_table.get_field(
            index_name="email", key_value=TEST_ACCOUNT_EMAIL, field_path="accountId"
        )
        self.assertEqual(account_id, TEST_ACCOUNT_ID)

        account_data: Optional[dict] = self.users_table.get_multiple_fields(
            index_name="email", key_value=TEST_ACCOUNT_EMAIL,
            getters={
                "accountId": FieldGetter(field_path="accountId"),
                "username": FieldGetter(field_path="username")
            }
        )
        self.assertEqual(account_data.get("accountId", None), TEST_ACCOUNT_ID)
        self.assertEqual(account_data.get("username", None), TEST_ACCOUNT_USERNAME)

    def test_set_data_inside_a_map_model_field(self):
        dummy_value = str(uuid4())

        set_update_success: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='testMapModel.sampleText',
            value_to_set=dummy_value
        )
        self.assertEqual(set_update_success, True)

        retrieved_value: Optional[str] = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='testMapModel.sampleText'
        )
        self.assertEqual(retrieved_value, dummy_value)

        delete_field_success: bool = self.users_table.delete_field(
            key_value=TEST_ACCOUNT_ID,
            field_path="testMapModel.sampleText"
        )
        self.assertEqual(delete_field_success, True)

    def test_set_dict_item_with_primitive_value(self):
        success_valid: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='testDictWithPrimitiveValue.{{key}}',
            query_kwargs={'key': "one"},
            value_to_set=True
        )
        self.assertTrue(success_valid)

        success_invalid: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='testDictWithPrimitiveValue.{{key}}',
            query_kwargs={'key': "one"},
            value_to_set={'keyOfDictThatShouldNotBeHere': True}
        )
        self.assertFalse(success_invalid)

if __name__ == '__main__':
    unittest.main()
