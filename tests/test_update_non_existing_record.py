import unittest
import uuid
from typing import Optional

from StructNoSQL import TableDataModel, BaseField
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable


class TableModel(TableDataModel):
    accountId = BaseField(field_type=str, required=True)
    username = BaseField(field_type=str, required=False)

class TestsUpdateNonExistingRecord(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

    def test_update_non_existing_record(self):
        random_record_id: str = f"randomRecordId_{uuid.uuid4()}"

        update_success: bool = self.users_table.update_field(
            key_value=random_record_id, field_path='username', value_to_set="Robinson"
        )
        self.assertTrue(update_success)

        retrieved_username: Optional[str] = self.users_table.get_field(
            key_value=random_record_id, field_path='username'
        )
        self.assertEqual("Robinson", retrieved_username)

        record_deletion_success: bool = self.users_table.delete_record({
            'accountId': random_record_id
        })
        self.assertTrue(record_deletion_success)

if __name__ == '__main__':
    unittest.main()
