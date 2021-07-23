from typing import Dict
from StructNoSQL import TableDataModel, BaseField, MapModel


class BaseTableModel(TableDataModel):
    value = BaseField(field_type=str, required=False)

class DynamoDBTableModel(BaseTableModel):
    accountId = BaseField(field_type=str, required=True)

class ExternalDynamoDBApiTableModel(BaseTableModel):
    accountProjectTableKeyId = BaseField(field_type=str, required=True)
