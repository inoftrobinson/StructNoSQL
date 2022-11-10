from StructNoSQL import TableDataModel, BaseField, MapModel


class DynamoDBTableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    simpleTextField = BaseField(field_type=str, required=False)
    class ContainerModel(MapModel):
        textFieldOne = BaseField(field_type=str, required=False)
        textFieldTwo = BaseField(field_type=str, required=False)
    container = BaseField(field_type=ContainerModel, required=False)
