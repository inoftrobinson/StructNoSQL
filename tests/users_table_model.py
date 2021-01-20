from typing import Dict
from StructNoSQL import TableDataModel, BaseField, MapModel, MapField, NoneType


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
