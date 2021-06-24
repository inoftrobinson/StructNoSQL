from typing import Dict
from StructNoSQL import TableDataModel, BaseField, MapModel


class BaseTableModel(TableDataModel):
    fieldOne = BaseField(name='fieldOne', field_type=str, required=False)
    fieldTwo = BaseField(name='fieldTwo', field_type=str, required=False)
    class ContainerModel(MapModel):
        fieldOne = BaseField(name='fieldOne', field_type=str, required=False)
        fieldTwo = BaseField(name='fieldTwo', field_type=str, required=False)
        fieldThree = BaseField(name='fieldThree', field_type=str, required=False)
    container = BaseField(name='container', field_type=Dict[str, ContainerModel], key_name='containerKey', required=False)

class DynamoDBTableModel(BaseTableModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)

class InoftVocalEngineTableModel(BaseTableModel):
    accountProjectUserId = BaseField(name='accountProjectUserId', field_type=str, required=True)
