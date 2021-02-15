import random
import unittest

from StructNoSQL import TableDataModel, BaseField
from tests.tests_caching_table.users_table import CachingUsersTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    simpleValue = BaseField(name='simpleValue', field_type=int, required=False)

class TestGetField(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = CachingUsersTable(data_model=TableModel)

    def test_simple_get_field(self):
        random_field_value = random.randint(0, 100)
        self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue', value_to_set=random_field_value)
        first_response = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        second_response = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')


if __name__ == '__main__':
    unittest.main()
