from typing import Dict
from StructNoSQL import TableDataModel, BaseField, MapModel


class DynamoDBTableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    type = BaseField(field_type=str, required=False)
    fieldOne = BaseField(field_type=str, required=False)
    fieldTwo = BaseField(field_type=str, required=False)
    class ContainerModel(MapModel):
        fieldOne = BaseField(field_type=str, required=False)
        fieldTwo = BaseField(field_type=str, required=False)
        fieldThree = BaseField(field_type=str, required=False)
    container = BaseField(field_type=Dict[str, ContainerModel], key_name='containerKey', required=False)
