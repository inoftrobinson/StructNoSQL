from typing import Dict, Optional, Any

from StructNoSQL import NoneType, BaseField, TableDataModel, MapModel, MapField, BaseTable, PrimaryIndex, GlobalSecondaryIndex


TEST_ACCOUNT_ID = "5ae5938d-d4b5-41a7-ad33-40f3c1476211"
TEST_PROJECT_ID = "defcc77c-1d6d-46a4-8cbe-506d12b824b7"
TEST_ACCOUNT_EMAIL = "yay.com"
TEST_ACCOUNT_USERNAME = "Yay"


class UsersTableModel(TableDataModel):
    accountId = BaseField(name="accountId", field_type=str, required=True)
    username = BaseField(name="username", field_type=str)
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
    def __init__(self, data_model: Optional[Any] = None):
        primary_index = PrimaryIndex(hash_key_name="accountId", hash_key_variable_python_type=str)
        globals_secondary_indexes = [
            GlobalSecondaryIndex(hash_key_name="username", hash_key_variable_python_type=str, projection_type="ALL"),
            GlobalSecondaryIndex(hash_key_name="email", hash_key_variable_python_type=str, projection_type="ALL"),
        ]
        super().__init__(table_name="structnosql-playground", region_name="eu-west-2", data_model=data_model or UsersTableModel(),
                         primary_index=primary_index, global_secondary_indexes=globals_secondary_indexes, auto_create_table=True)
