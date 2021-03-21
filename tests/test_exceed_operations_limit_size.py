import unittest
from sys import getsizeof
from typing import List, Dict

from StructNoSQL import FieldRemover, BaseField, MapModel, TableDataModel
from StructNoSQL.dynamodb.dynamodb_core import EXPRESSION_MAX_BYTES_SIZE
from tests.users_table import UsersTable, TEST_ACCOUNT_ID


class TestExceedOperationsLimitSize(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)

    def test_deletion_and_removal_limit_size(self):
        class TableModel(TableDataModel):
            accountId = BaseField(name='accountId', field_type=str, required=True)
            container = BaseField(name='dummy', field_type=bool, required=False)

        table_model = TableModel()
        removers: Dict[str, FieldRemover] = dict()

        index = 0
        while True:
            current_field_path = f"dummy{index}"
            removers[current_field_path] = FieldRemover(field_path=current_field_path)
            table_model.class_add_field(
                field_key=current_field_path,
                field_item=BaseField(name=current_field_path, field_type=bool, required=False)
            )
            if getsizeof(removers) > (EXPRESSION_MAX_BYTES_SIZE * 1.5):
                break
            index += 1

        users_table = UsersTable(data_model=table_model)
        # We initialize the table after having added all the fields to the class of the TableModel, so that
        # the indexing of the model will correctly be done on all the programmatically added fields.

        deletion_response = users_table.delete_multiple_fields(key_value=TEST_ACCOUNT_ID, removers=removers)
        self.assertTrue(all(deletion_response.values()))

        retrieved_removed_data = users_table.remove_multiple_fields(key_value=TEST_ACCOUNT_ID, removers=removers)
        self.assertIsNotNone(retrieved_removed_data)


if __name__ == '__main__':
    unittest.main()
