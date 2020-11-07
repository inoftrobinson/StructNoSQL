import unittest
from dataclasses import dataclass
from typing import Dict, List, Optional
from uuid import uuid4

from StructNoSQL import BaseTable, BaseField, MapModel, MapField, TableDataModel, PrimaryIndex, GlobalSecondaryIndex, \
    NoneType, FieldGetter, FieldSetter, FieldRemover
from StructNoSQL.exceptions import FieldTargetNotFoundException
from StructNoSQL.practical_logger import message_with_vars


class UsersTableModel(TableDataModel):
    accountId = BaseField(name="accountId", field_type=str, required=True)
    username = BaseField(name="accountUsername", field_type=str)
    class ProjectModel(MapModel):
        projectName = BaseField(name="projectName", field_type=str, required=True)
        class InstancesInfosModel(MapModel):
            ya = BaseField(name="ya", field_type=str)
        instancesInfos = MapField(name="instancesInfos", model=InstancesInfosModel)
    projects = BaseField(name="projects", field_type=Dict[str, ProjectModel], key_name="projectId")
    multiTypes = BaseField(name="multiTypes", field_type=[str, NoneType], required=True)
    number1 = BaseField(name="number1", field_type=[int, float], required=False)
    string1 = BaseField(name="string1", field_type=str, required=False)
    floatTest = BaseField(name="floatTest", field_type=float, required=False)

    fieldToRemove = BaseField(name="fieldToRemove", field_type=str, required=False)
    class SophisticatedRemovalModel(MapModel):
        nestedVariable = BaseField(name="nestedVariable", field_type=str, required=False)
    sophisticatedRemoval = BaseField(name="sophisticatedRemoval", field_type=Dict[str, SophisticatedRemovalModel], key_name="id", required=False)

    class TestMapModel(MapModel):
        sampleText = BaseField(name="sampleText", field_type=str, required=False)
    testMapModel = MapField(name="testMapModel", model=TestMapModel)

    testDictWithPrimitiveValue = BaseField(name="testDictWithPrimitiveValue", field_type=Dict[str, bool], key_name="key")


class UsersTable(BaseTable):
    def __init__(self):
        primary_index = PrimaryIndex(hash_key_name="accountId", hash_key_variable_python_type=str)
        globals_secondary_indexes = [
            GlobalSecondaryIndex(hash_key_name="username", hash_key_variable_python_type=str, projection_type="ALL"),
            GlobalSecondaryIndex(hash_key_name="email", hash_key_variable_python_type=str, projection_type="ALL"),
        ]
        super().__init__(table_name="inoft-vocal-engine_accounts-data_dev", region_name="eu-west-2", data_model=UsersTableModel(),
                         primary_index=primary_index, global_secondary_indexes=globals_secondary_indexes, auto_create_table=True)


class TestTableOperations(unittest.TestCase):
    def __init__(self, methodName: str):
        super().__init__(methodName=methodName)
        self.users_table = UsersTable()
        self.test_account_id = "5ae5938d-d4b5-41a7-ad33-40f3c1476211"
        self.test_project_id = "defcc77c-1d6d-46a4-8cbe-506d12b824b7"
        self.test_account_email = "yay.com"
        self.test_account_username = "Yay"

    def test_get_all_projects(self):
        response_items: Optional[List[dict]] = self.users_table.query(
            key_name="accountId", key_value=self.test_account_id, fields_to_get=["projects"]
        )
        if response_items is not None:
            for item in response_items:
                current_item_projects = item.get("projects", None)
                if current_item_projects is not None:
                    for project_data in current_item_projects.values():
                        print(f"Project : {project_data}")
                        project_name = project_data.get("projectName", None)
                        if project_name is not None:
                            self.assertIn("test", project_data["projectName"])

    def test_get_name_of_one_project(self):
        response_data: Optional[str] = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            field_to_get="projects.{{projectId}}.projectName",
            query_kwargs={"projectId": self.test_project_id}
        )
        self.assertIn("test", response_data)

    def test_update_project_name(self):
        success = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="projects.{{projectId}}.projectName",
            query_kwargs={"projectId": self.test_project_id},
            value_to_set="test2"
        )
        self.assertTrue(success)

        response_data: Optional[str] = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            field_to_get="projects.{{projectId}}.projectName",
            query_kwargs={"projectId": self.test_project_id}
        )
        self.assertEqual(response_data, "test2")

    def test_update_entire_project_model(self):
        success = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="projects.{{projectId}}", value_to_set={"projectName": "test3"},
            query_kwargs={"projectId": self.test_project_id}
        )
        # todo: allow to set the item of a dict (currently, when doing a query on the projects object,
        #  we will perform an operation of the project map, and not on an individual project item).
        self.assertTrue(success)

        response_data: Optional[str] = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            field_to_get="projects.{{projectId}}.projectName",
            query_kwargs={"projectId": self.test_project_id}
        )
        self.assertEqual(response_data, "test3")

    def test_update_entire_project_model_with_missing_project_name(self):
        success = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="projects.{{projectId}}", value_to_set={},
            query_kwargs={"projectId": self.test_project_id}
        )
        self.assertFalse(success)

    def test_update_entire_project_model_with_invalid_data(self):
        success = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="projects.{{projectId}}", value_to_set={"invalidProjectName": "test4"},
            query_kwargs={"projectId": self.test_project_id}
        )
        self.assertFalse(success)

        try:
            response_data: Optional[str] = self.users_table.get_single_field_value_from_single_item(
                key_name="accountId", key_value=self.test_account_id,
                field_to_get="projects.{{projectId}}.invalidProjectName",
                query_kwargs={"projectId": self.test_project_id}
            )
            # If we do not get an error while trying to access an invalid field in the
            # get_single_field_value_from_single_item function, then we failed the test.
            self.fail()
        except FieldTargetNotFoundException as e:
            print(e)

    def test_multi_types_field(self):
        success_str = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="multiTypes", value_to_set="yolo"
        )
        self.assertTrue(success_str)

        success_none = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="multiTypes", value_to_set=None
        )
        self.assertTrue(success_none)

        success_bool = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="multiTypes", value_to_set=True
        )
        self.assertFalse(success_bool)

    def test_basic_get_multiple_fields_values_in_single_query(self):
        response_data: Optional[dict] = self.users_table.get_multiple_fields_values_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            getters={
                "theAccountId": FieldGetter(target_path="accountId"),
                "theProjects": FieldGetter(target_path="projects")
            }
        )
        self.assertIsNotNone(response_data)
        self.assertEqual(response_data.get("theAccountId", None), self.test_account_id)

    def test_basic_get_multiple_fields_items_in_single_query(self):
        response_data: Optional[dict] = self.users_table.get_multiple_fields_items_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            getters={
                "theAccountId": FieldGetter(target_path="accountId"),
                "theProjects": FieldGetter(target_path="projects")
            }
        )
        self.assertIsNotNone(response_data)
        self.assertEqual(response_data.get("theAccountId", None), {"accountId": self.test_account_id})

    def test_sophisticated_get_multiple_fields_in_single_query(self):
        response_data: Optional[dict] = self.users_table.get_multiple_fields_values_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            getters={
                "theAccountId": FieldGetter(target_path="accountId"),
                "theProjectName": FieldGetter(
                    target_path="projects.{{projectId}}.projectName",
                    query_kwargs={"projectId": self.test_project_id}
                )
            }
        )
        self.assertIsNotNone(response_data)
        self.assertEqual(response_data.get("theAccountId", None), self.test_account_id)
        self.assertIn("test", response_data.get("theProjectName", ""))

    def test_basic_set_update_multiple_fields_in_single_query(self):
        success: bool = self.users_table.set_update_multiple_fields(
            key_name="accountId", key_value=self.test_account_id,
            setters=[
                FieldSetter(target_path="number1", value_to_set=42),
                FieldSetter(target_path="string1", value_to_set="Quarante-deux")
            ]
        )
        self.assertTrue(success)

    def test_sophisticated_set_update_multiple_fields_in_single_query(self):
        success: bool = self.users_table.set_update_multiple_fields(
            key_name="accountId", key_value=self.test_account_id,
            setters=[
                FieldSetter(target_path="number1", value_to_set=42),
                FieldSetter(
                    target_path="projects.{{projectId}}.projectName",
                    query_kwargs={"projectId": self.test_project_id},
                    value_to_set="test5",
                )
            ]
        )
        self.assertTrue(success)

        project_name_data: Optional[str] = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            field_to_get="projects.{{projectId}}.projectName",
            query_kwargs={"projectId": self.test_project_id},
        )
        self.assertEqual(project_name_data, "test5")

    def test_set_and_get_float_in_field_value(self):
        source_float_value = 10.42021023492

        set_float_value_success = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="floatTest", value_to_set=source_float_value
        )
        self.assertTrue(set_float_value_success)

        retrieved_float_value: Optional[float] = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id, field_to_get="floatTest"
        )
        self.assertEqual(retrieved_float_value, source_float_value)

    def test_remove_basic_item_from_path_target(self):
        success_field_set = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="fieldToRemove", value_to_set="yolo mon ami !"
        )
        self.assertTrue(success_field_set)

        success_field_remove = self.users_table.remove_single_item_at_path_target(
            key_name="accountId", key_value=self.test_account_id,
            target_field="fieldToRemove"
        )
        self.assertTrue(success_field_remove)

        retrieved_field_data = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            field_to_get="fieldToRemove"
        )
        self.assertIsNone(retrieved_field_data)

    def test_remove_sophisticated_item_from_path_target(self):
        query_kwargs = {"id": "sampleId"}

        success_field_set = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="sophisticatedRemoval.{{id}}.nestedVariable",
            query_kwargs=query_kwargs,
            value_to_set="Soooooo, does it works ? :)"
        )
        self.assertTrue(success_field_set)

        success_field_remove = self.users_table.remove_single_item_at_path_target(
            key_name="accountId", key_value=self.test_account_id,
            target_field="sophisticatedRemoval.{{id}}.nestedVariable",
            query_kwargs=query_kwargs
        )
        self.assertTrue(success_field_remove)

        retrieved_field_data = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            field_to_get="sophisticatedRemoval.{{id}}.nestedVariable",
            query_kwargs=query_kwargs
        )
        self.assertIsNone(retrieved_field_data)

    def test_remove_multiple_basic_and_sophisticated_items_from_path_target(self):
        query_kwargs = {"id": "sampleIdTwo"}

        success_fields_set = self.users_table.set_update_multiple_fields(
            key_name="accountId", key_value=self.test_account_id,
            setters=[
                FieldSetter(
                    target_path="fieldToRemove",
                    value_to_set="multipleBasicAndSophisticatedRemoval"
                ),
                FieldSetter(
                    target_path="sophisticatedRemoval.{{id}}.nestedVariable",
                    query_kwargs=query_kwargs, value_to_set="nestedDude"
                )
            ]
        )
        self.assertTrue(success_fields_set)

        success_fields_remove = self.users_table.remove_multiple_items_at_path_targets(
            key_name="accountId", key_value=self.test_account_id,
            removers=[
                FieldRemover(target_path="fieldToRemove"),
                FieldRemover(target_path="sophisticatedRemoval.{{id}}.nestedVariable", query_kwargs=query_kwargs)
            ]
        )
        self.assertTrue(success_fields_remove)

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
        account_id: Optional[str] = self.users_table.get_single_field_value_from_single_item(
            key_name="email", key_value=self.test_account_email, field_to_get="accountId"
        )
        self.assertEqual(account_id, self.test_account_id)

        account_data: Optional[dict] = self.users_table.get_multiple_fields_values_from_single_item(
            key_name="email", key_value=self.test_account_email,
            getters={
                "accountId": FieldGetter(target_path="accountId"),
                "accountUsername": FieldGetter(target_path="accountUsername")
            }
        )
        self.assertEqual(account_data.get("accountId", None), self.test_account_id)
        self.assertEqual(account_data.get("accountUsername", None), self.test_account_username)

    def test_set_data_inside_a_map_model_field(self):
        dummy_value = str(uuid4())

        set_update_success = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="testMapModel.sampleText", value_to_set=dummy_value
        )
        self.assertEqual(set_update_success, True)

        retrieved_value = self.users_table.get_single_field_value_from_single_item(
            key_name="accountId", key_value=self.test_account_id,
            field_to_get="testMapModel.sampleText"
        )
        self.assertEqual(retrieved_value, dummy_value)

        remove_field_success = self.users_table.remove_single_item_at_path_target(
            key_name="accountId", key_value=self.test_account_id,
            target_field="testMapModel.sampleText"
        )
        self.assertEqual(remove_field_success, True)

    def test_set_dict_item_with_primitive_value(self):
        success_valid = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="testDictWithPrimitiveValue.{{key}}",
            query_kwargs={"key": "one"}, value_to_set=True
        )
        self.assertTrue(success_valid)

        success_invalid = self.users_table.set_update_one_field(
            key_name="accountId", key_value=self.test_account_id,
            target_field="testDictWithPrimitiveValue.{{key}}",
            query_kwargs={"key": "one"},
            value_to_set={"keyOfDictThatShouldNotBeHere": True}
        )
        self.assertFalse(success_invalid)

if __name__ == '__main__':
    unittest.main()
