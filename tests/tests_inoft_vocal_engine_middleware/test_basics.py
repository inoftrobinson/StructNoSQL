import unittest
from typing import List

from StructNoSQL import TableDataModel, BaseField, MapModel
from tests.tests_inoft_vocal_engine_middleware.inoft_vocal_engine_caching_users_table import \
    InoftVocalEngineCachingUsersTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    simpleValue = BaseField(name='simpleValue', field_type=int, required=False)
    simpleValue2 = BaseField(name='simpleValue2', field_type=int, required=False)
    fieldToDelete = BaseField(name='fieldToDelete', field_type=int, required=False)
    fieldToDelete2 = BaseField(name='fieldToDelete2', field_type=int, required=False)
    fieldToRemove = BaseField(name='fieldToRemove', field_type=int, required=False)
    fieldToRemove2 = BaseField(name='fieldToRemove2', field_type=int, required=False)
    class ContainerToRemoveModel(MapModel):
        fieldOne = BaseField(name='fieldOne', field_type=str, required=False)
        fieldTwo = BaseField(name='fieldTwo', field_type=str, required=False)
        fieldThree = BaseField(name='fieldThree', field_type=str, required=False)
    containerToRemove = BaseField(name='containerToRemove', field_type=ContainerToRemoveModel, required=False)
    containersListToRemove = BaseField(name='containersListToRemove', field_type=List[ContainerToRemoveModel], key_name='listIndex')

class TestAllInoftVocalEngineCachingTable(unittest.TestCase):
    def reset_table(self):
        self.users_table = InoftVocalEngineCachingUsersTable(data_model=TableModel)
        self.users_table.debug = True

    def test_simple_get_field(self):
        self.reset_table()

        first_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual(first_response_data['fromCache'], False)

        second_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual(second_response_data['fromCache'], True)
