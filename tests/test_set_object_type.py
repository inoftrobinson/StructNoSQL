import unittest
from typing import Set, Optional
from uuid import uuid4

from StructNoSQL import BaseField, MapModel, TableDataModel, MapField
from StructNoSQL.exceptions import UsageOfUntypedSetException
from tests.users_table import UsersTable, TEST_ACCOUNT_ID, TEST_PROJECT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    class ContainerModel(MapModel):
        typedSet = BaseField(name='typedSet', field_type=Set[str], key_name='setKey', required=False)
    container = MapField(name='container', model=ContainerModel)

class TestsSetObjectType(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = UsersTable(data_model=TableModel())

    def test_crash_on_untyped_set(self):
        def init_table():
            class TableModel:
                accountId = BaseField(name='accountId', field_type=str, required=True)
                untypedSet = BaseField(name='untypedSet', field_type=set, key_name='setKey', required=False)
            users_table = UsersTable(data_model=TableModel())
        self.assertRaises(UsageOfUntypedSetException, init_table)

    def test_set_retrieve_individual_typed_set_item(self):
        valid_random_set_values: Set[str] = set(str(uuid4()) for i in range(5))
        single_valid_set_item = list(valid_random_set_values)[2]
        random_set_values = valid_random_set_values.copy()
        random_set_values.add(42)
        # Add invalid value to the random_set_values

        set_success = self.users_table.set_update_one_field_value_in_single_record(
            key_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedSet', value_to_set=random_set_values
        )
        self.assertTrue(set_success)

        # todo: add support for retrieving single set item (we need to check if the set item exists)
        retrieved_set_item: Optional[str] = self.users_table.get_one_field_value_from_single_record(
            key_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedSet.{{setKey}}', query_kwargs={'setKey': "ee"}
        )
        self.assertEqual(retrieved_set_item, single_valid_set_item)

        retrieved_entire_set: Optional[Set[str]] = self.users_table.get_one_field_value_from_single_record(
            key_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedSet',
        )
        self.assertEqual(valid_random_set_values, retrieved_entire_set)

        deletion_success = self.users_table.remove_one_field_item_in_single_record(
            key_name='accountId', key_value=TEST_ACCOUNT_ID,
            field_path='container.typedSet'
        )
        self.assertTrue(deletion_success)


if __name__ == '__main__':
    unittest.main()
