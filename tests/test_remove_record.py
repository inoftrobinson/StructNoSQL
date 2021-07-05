import unittest
import uuid
from typing import Optional

from StructNoSQL import BaseField, MapModel, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    value = BaseField(name='value', field_type=str, required=False)


class MyTestCase(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

    def test_something(self):
        random_record_id: str = f"recordId_{uuid.uuid4()}"
        random_value: str = f"value_{uuid.uuid4()}"
        put_record_success: bool = self.users_table.put_record(record_dict_data={
            'accountId': random_record_id, 'value': random_value
        })
        self.assertTrue(put_record_success)

        removed_record_data: Optional[dict] = self.users_table.remove_record(indexes_keys_selectors={'accountId': random_record_id})
        self.assertEqual({'accountId': random_record_id, 'value': random_value}, removed_record_data)


if __name__ == '__main__':
    unittest.main()
