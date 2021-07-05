import unittest
from typing import Optional, Dict
from uuid import uuid4

from StructNoSQL import BaseField, FieldRemover, TableDataModel
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID


class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    rootFieldOne = BaseField(name='rootFieldOne', field_type=str, required=False)
    rootFieldTwo = BaseField(name='rootFieldTwo', field_type=str, required=False)


class TestMultiRetrievalOfRootFilesWithMissingData(unittest.TestCase):
    """
    This test the main bug fix released in StructNoSQL 1.5.4, where if multiple root fields were requested
    (example, rootFieldOne and rootFieldTwo), if one of the field was found and got a value, but the other
    one was not found, and so not included in the response_data dict, we would end up with a response data
    with only key. Prior to 1.5.4, StructNoSQL would treat any returned dictionary with a single key as a 
    DynamoDB data container, and would try to convert it. Usually, a wrong conversion like this would not
    cause any problems, unless there was another field we requested, in which case, the returned data would
    be navigated into too much by StructNoSQL, and would then be used to populate the multiple requested
    fields (ie, populate the rootFieldOne value in both rootFieldOne and rootFieldTwo, where rootFieldTwo
    should be None). StructNoSQL 1.5.4 should have fixed this issue by not treating any dict with a single
    key as a DynamoDB container, but only dict with a single key that is a DynamoDB data types keys (ie: 'S',
    'BOOL', 'NULL', etc), and by now considering all the DynamoDB data types keys as restricted fields names.
    """

    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundDynamoDBBasicTable(data_model=TableModel)

    def test_with_two_root_fields(self):
        deletions_successes: Dict[str, bool] = self.users_table.delete_multiple_fields(
            key_value=TEST_ACCOUNT_ID, removers={
                'one': FieldRemover(field_path='rootFieldOne'),
                'two': FieldRemover(field_path='rootFieldTwo')
            }
        )

        field_one_random_value = f"field1_{uuid4()}"
        field_one_update_success: bool = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path='rootFieldOne', value_to_set=field_one_random_value
        )
        self.assertTrue(field_one_update_success)

        retrieved_data: Optional[dict] = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID, field_path='(rootFieldOne, rootFieldTwo)'
        )
        self.assertIsNotNone(retrieved_data)
        self.assertEqual(retrieved_data.get('rootFieldOne', None), field_one_random_value)
        self.assertEqual(retrieved_data.get('rootFieldTwo', None), None)


if __name__ == '__main__':
    unittest.main()
