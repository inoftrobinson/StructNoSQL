from typing import Dict
from StructNoSQL import TableDataModel, BaseField, MapModel


class BaseTableModel(TableDataModel):
    value = BaseField(name='value', field_type=str, required=False)

class DynamoDBTableModel(BaseTableModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)

class InoftVocalEngineTableModel(BaseTableModel):
    accountProjectUserId = BaseField(name='accountProjectUserId', field_type=str, required=True)
