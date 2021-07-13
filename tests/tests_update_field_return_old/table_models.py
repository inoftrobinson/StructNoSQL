from typing import Dict
from StructNoSQL import TableDataModel, BaseField, MapModel


class BaseTableModel(TableDataModel):
    simpleTextField = BaseField(field_type=str, required=False)
    class SimpleContainerModel(MapModel):
        textFieldOne = BaseField(field_type=str, required=False)
        textFieldTwo = BaseField(field_type=str, required=False)
    container = BaseField(field_type=SimpleContainerModel, required=False)

class DynamoDBTableModel(BaseTableModel):
    accountId = BaseField(field_type=str, required=True)

class InoftVocalEngineTableModel(BaseTableModel):
    accountProjectUserId = BaseField(field_type=str, required=True)
