import unittest
from StructNoSQL import BaseField, TableDataModel
from StructNoSQL.exceptions import InvalidFieldNameException
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable


class TestReservedChars(unittest.TestCase):
    def test_left_bracket_char(self):
        def init_table():
            class TableModel(TableDataModel):
                accountId = BaseField(name='accountId', field_type=str, required=True)
                restrictedRightBracket = BaseField(name='restricted_[e', field_type=str, required=False)
            users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)
        self.assertRaises(InvalidFieldNameException, init_table)

    def test_right_bracket_char(self):
        def init_table():
            class TableModel(TableDataModel):
                accountId = BaseField(name='accountId', field_type=str, required=True)
                restrictedLeftBracket = BaseField(name='restricted_]e', field_type=str, required=False)
            users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)
        self.assertRaises(InvalidFieldNameException, init_table)

    def test_left_curly_bracket_char(self):
        def init_table():
            class TableModel(TableDataModel):
                accountId = BaseField(name='accountId', field_type=str, required=True)
                restrictedRightBracket = BaseField(name='restricted_{e', field_type=str, required=False)
            users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)
        self.assertRaises(InvalidFieldNameException, init_table)

    def test_right_curly_bracket_char(self):
        def init_table():
            class TableModel(TableDataModel):
                accountId = BaseField(name='accountId', field_type=str, required=True)
                restrictedLeftBracket = BaseField(name='restricted_}e', field_type=str, required=False)
            users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)
        self.assertRaises(InvalidFieldNameException, init_table)

    def test_left_parenthesis_char(self):
        def init_table():
            class TableModel(TableDataModel):
                accountId = BaseField(name='accountId', field_type=str, required=True)
                restrictedRightBracket = BaseField(name='restricted_(e', field_type=str, required=False)
            users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)
        self.assertRaises(InvalidFieldNameException, init_table)

    def test_right_parenthesis_char(self):
        def init_table():
            class TableModel(TableDataModel):
                accountId = BaseField(name='accountId', field_type=str, required=True)
                restrictedLeftBracket = BaseField(name='restricted_)e', field_type=str, required=False)
            users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)
        self.assertRaises(InvalidFieldNameException, init_table)

    def test_separator_char(self):
        def init_table():
            class TableModel(TableDataModel):
                accountId = BaseField(name='accountId', field_type=str, required=True)
                restrictedLeftBracket = BaseField(name='restricted_|e', field_type=str, required=False)
            users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)
        self.assertRaises(InvalidFieldNameException, init_table)


if __name__ == '__main__':
    unittest.main()
