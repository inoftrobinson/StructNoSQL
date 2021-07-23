from StructNoSQL import TableDataModel, BaseField


class BaseTableModel(TableDataModel):
    simpleField = BaseField(field_type=str, required=False)
    fieldWithCustomName = BaseField(custom_field_name='field%/!$', field_type=str, required=False)

class DynamoDBTableModel(BaseTableModel):
    accountId = BaseField(field_type=str, required=True)

class ExternalDynamoDBApiTableModel(BaseTableModel):
    accountProjectTableKeyId = BaseField(field_type=str, required=True)
