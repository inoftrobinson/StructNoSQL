from typing import List
from StructNoSQL import TableDataModel, BaseField, MapModel


class BaseTableModel(TableDataModel):
    simpleValue = BaseField(name='simpleValue', field_type=int, required=False)
    simpleValue2 = BaseField(name='simpleValue2', field_type=int, required=False)
    fieldOne = BaseField(name='fieldOne', field_type=str, required=False)
    fieldTwo = BaseField(name='fieldTwo', field_type=str, required=False)
    fieldToDelete = BaseField(name='fieldToDelete', field_type=int, required=False)
    fieldToDelete2 = BaseField(name='fieldToDelete2', field_type=int, required=False)
    fieldToRemove = BaseField(name='fieldToRemove', field_type=int, required=False)
    fieldToRemove2 = BaseField(name='fieldToRemove2', field_type=int, required=False)
    class ContainerToRemoveModel(MapModel):
        fieldOne = BaseField(name='fieldOne', field_type=str, required=False)
        fieldTwo = BaseField(name='fieldTwo', field_type=str, required=False)
        fieldThree = BaseField(name='fieldThree', field_type=str, required=False)
    containerToRemove = BaseField(name='containerToRemove', field_type=ContainerToRemoveModel, required=False)
    containersListToRemove = BaseField(name='containersListToRemove', field_type=List[ContainerToRemoveModel], key_name='listIndex')

class DynamoDBTableModel(BaseTableModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)

class InoftVocalEngineTableModel(BaseTableModel):
    accountProjectUserId = BaseField(name='accountProjectUserId', field_type=str, required=True)
