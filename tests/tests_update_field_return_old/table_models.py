from StructNoSQL import TableDataModel, BaseField, MapModel


class BaseTableModel(TableDataModel):
    simpleTextField = BaseField(field_type=str, required=False)
    class ContainerModel(MapModel):
        textFieldOne = BaseField(field_type=str, required=False)
        textFieldTwo = BaseField(field_type=str, required=False)
    container = BaseField(field_type=ContainerModel, required=False)

class DynamoDBTableModel(BaseTableModel):
    accountId = BaseField(field_type=str, required=True)

class ExternalDynamoDBApiTableModel(BaseTableModel):
    accountProjectUserId = BaseField(field_type=str, required=True)
