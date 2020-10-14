import unittest
from dataclasses import dataclass
from typing import Dict, List, Optional
from StructNoSQL import BaseTable, BaseField, MapModel, MapField, TableDataModel, PrimaryIndex, GlobalSecondaryIndex, \
    NoneType, FieldGetter, FieldSetter
from StructNoSQL.exceptions import FieldTargetNotFoundException
from StructNoSQL.practical_logger import message_with_vars


class UsersTableModel(TableDataModel):
    accountId = BaseField(name="accountId", field_type=str)
    class ProjectModel(MapModel):
        projectName = BaseField(name="projectName", field_type=str, required=True)
        class InstancesInfosModel(MapModel):
            ya = BaseField(name="ya", field_type=str)
        instancesInfos = MapField(name="instancesInfos", model=InstancesInfosModel)
    projects = BaseField(name="projects", field_type=Dict[str, ProjectModel], key_name="projectId")
    multiTypes = BaseField(name="multiTypes", field_type=[str, NoneType], required=True)
    number1 = BaseField(name="number1", field_type=[int, float], required=False)
    string1 = BaseField(name="string1", field_type=str, required=False)


class UsersTable(BaseTable):
    def __init__(self):
        primary_index = PrimaryIndex(hash_key_name="accountId", hash_key_variable_python_type=str)
        globals_secondary_indexes = [
            GlobalSecondaryIndex(hash_key_name="username", hash_key_variable_python_type=str, projection_type="ALL"),
            GlobalSecondaryIndex(hash_key_name="email", hash_key_variable_python_type=str, projection_type="ALL"),
        ]
        super().__init__(table_name="inoft-vocal-engine_accounts-data", region_name="eu-west-2", data_model=UsersTableModel(),
                         primary_index=primary_index, global_secondary_indexes=globals_secondary_indexes, auto_create_table=True)


class TestTableOperations(unittest.TestCase):
    def __init__(self, methodName: str):
        super().__init__(methodName=methodName)
        self.users_table = UsersTable()
        self.test_account_id = "5ae5938d-d4b5-41a7-ad33-40f3c1476211"
        self.test_project_id = "defcc77c-1d6d-46a4-8cbe-506d12b824b7"

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


if __name__ == '__main__':
    unittest.main()