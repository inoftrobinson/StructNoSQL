import unittest
from uuid import uuid4

from StructNoSQL import BaseField, MapModel, NoneType, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    class ItemModel(MapModel):
        value = BaseField(name="value", field_type=str, required=True)
    requiredModelAcceptingNone = BaseField(name='requiredModelAcceptingNone', field_type=(ItemModel, NoneType), required=True)


class TestFieldsWithMultipleTypes(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

    def test_required_model_allowing_none(self):
        random_value_one: str = str(uuid4())
        update_one_success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path='requiredModelAcceptingNone',
            value_to_set={'value': random_value_one}
        )
        self.assertTrue(update_one_success)

        update_two_success: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path='requiredModelAcceptingNone',
            value_to_set="dummyDataThatShouldBeRejected"
        )
        self.assertFalse(update_two_success)

        update_three_success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path='requiredModelAcceptingNone',
            value_to_set=None
        )
        self.assertTrue(update_three_success)


if __name__ == '__main__':
    unittest.main()
