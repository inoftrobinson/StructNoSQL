from StructNoSQL import TableDataModel, BaseField


class DynamoDBTableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    value = BaseField(field_type=str, required=False)
