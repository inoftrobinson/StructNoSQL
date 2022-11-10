from StructNoSQL import TableDataModel, BaseField, MapModel


class BaseTableModel(TableDataModel):
    simpleFloatField = BaseField(field_type=float, required=False)
    class ContainerModel(MapModel):
        floatOne = BaseField(field_type=float, required=False)
        floatTwo = BaseField(field_type=float, required=False)
    floatsContainer = BaseField(field_type=ContainerModel, required=False)

class DynamoDBTableModel(BaseTableModel):
    accountId = BaseField(field_type=str, required=True)