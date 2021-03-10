import unittest
from typing import Set, Optional, Dict
from uuid import uuid4

from StructNoSQL import BaseField, MapModel, TableDataModel, MapField
from StructNoSQL.exceptions import UsageOfUntypedSetException
from tests.users_table import UsersTable, TEST_ACCOUNT_ID, TEST_PROJECT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    buildsResourcesConsumers = BaseField(name='buildsResourcesConsumers', field_type=Dict[str, Dict[str, bool]], required=False, key_name='resourceKey')


class TestsNestedStructuresInsideStructureValues(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = UsersTable(data_model=TableModel())

    def test_set_retrieve_individual_typed_set_item(self):
        keys_fields_switch = list(self.users_table.fields_switch.keys())
        self.assertIn('buildsResourcesConsumers.{{resourceKey}}.{{resourceKeyChild}}', keys_fields_switch)

        update_success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID,
            field_path='buildsResourcesConsumers.{{resourceKey}}.{{resourceKeyChild}}',
            query_kwargs={'resourceKey': 'parentKey', 'resourceKeyChild': 'childKey'},
            value_to_set=True
        )
        self.assertTrue(update_success)


if __name__ == '__main__':
    unittest.main()
