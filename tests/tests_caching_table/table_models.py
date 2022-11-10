from typing import List
from StructNoSQL import TableDataModel, BaseField, MapModel


class BaseTableModel(TableDataModel):
    simpleValue = BaseField(field_type=int, required=False)
    simpleValue2 = BaseField(field_type=int, required=False)
    fieldOne = BaseField(field_type=str, required=False)
    fieldTwo = BaseField(field_type=str, required=False)
    fieldToDelete = BaseField(field_type=int, required=False)
    fieldToDelete2 = BaseField(field_type=int, required=False)
    fieldToRemove = BaseField(field_type=int, required=False)
    fieldToRemove2 = BaseField(field_type=int, required=False)
    class ContainerToRemoveModel(MapModel):
        fieldOne = BaseField(field_type=str, required=False)
        fieldTwo = BaseField(field_type=str, required=False)
        fieldThree = BaseField(field_type=str, required=False)
    containerToRemove = BaseField(field_type=ContainerToRemoveModel, required=False)
    containersListToRemove = BaseField(field_type=List[ContainerToRemoveModel], key_name='listIndex')

class DynamoDBTableModel(BaseTableModel):
    accountId = BaseField(field_type=str, required=True)
