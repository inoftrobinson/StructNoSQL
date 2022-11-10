from StructNoSQL import TableDataModel, BaseField, MapModel


class BaseFirstTableModel(TableDataModel):
    simpleField = BaseField(field_type=str, required=False)
    class ContainerModel(MapModel):
        nestedFieldOne = BaseField(field_type=str, required=False)
        nestedFieldTwo = BaseField(field_type=str, required=False)
    container = BaseField(field_type=ContainerModel, required=False)

class BaseSecondTableModel(TableDataModel):
    simpleField = BaseField(field_type=int, required=False)
    class ContainerModel(MapModel):
        nestedFieldOne = BaseField(field_type=int, required=False)
        nestedFieldTwo = BaseField(field_type=int, required=False)
    container = BaseField(field_type=ContainerModel, required=False)


class DynamoDBFirstTableModel(BaseFirstTableModel):
    accountId = BaseField(field_type=str, required=True)
class DynamoDBSecondTableModel(BaseSecondTableModel):
    accountId = BaseField(field_type=str, required=True)
