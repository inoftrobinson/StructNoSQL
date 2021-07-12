from typing import Dict
from StructNoSQL import TableDataModel, BaseField, MapModel, NoneType


class UsersTableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    username = BaseField(field_type=str)
    class ProjectModel(MapModel):
        projectName = BaseField(field_type=str, required=True)
        class InstancesInfosModel(MapModel):
            ya = BaseField(field_type=str)
        instancesInfos = BaseField(field_type=InstancesInfosModel, required=False)
    projects = BaseField(field_type=Dict[str, ProjectModel], key_name="projectId")
    multiTypes = BaseField(field_type=[str, NoneType], required=True)
    number1 = BaseField(field_type=[int, float], required=False)
    string1 = BaseField(field_type=str, required=False)
    floatTest = BaseField(field_type=float, required=False)

    fieldToRemove = BaseField(field_type=str, required=False)
    fieldToDelete = BaseField(field_type=str, required=False)
    class SophisticatedRemovalModel(MapModel):
        nestedVariable = BaseField(field_type=str, required=False)
    sophisticatedRemoval = BaseField(field_type=Dict[str, SophisticatedRemovalModel], key_name="id", required=False)

    class TestMapModel(MapModel):
        sampleText = BaseField(field_type=str, required=False)
    testMapModel = BaseField(field_type=TestMapModel, required=False)

    testDictWithPrimitiveValue = BaseField(field_type=Dict[str, bool], key_name="key")
