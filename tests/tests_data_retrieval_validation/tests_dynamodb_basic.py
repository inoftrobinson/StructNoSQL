import unittest

from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable
from tests.tests_data_retrieval_validation.table_models import DynamoDBFirstTableModel, DynamoDBSecondTableModel


class TestsDynamoDBBasicTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.first_table = PlaygroundDynamoDBBasicTable(data_model=DynamoDBFirstTableModel)
        self.second_table = PlaygroundDynamoDBBasicTable(data_model=DynamoDBSecondTableModel)

        self.SHARED_CASE_KWARGS = {
            'self': self, 'first_table': self.first_table, 'second_table': self.second_table,
            'is_caching': False, 'primary_key_name': 'accountId'
        }

    def test_get_field(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_get_field
        test_get_field(**self.SHARED_CASE_KWARGS)

    def test_get_field_multi_selectors(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_get_field_multi_selectors
        test_get_field_multi_selectors(**self.SHARED_CASE_KWARGS)

    def test_get_multiple_fields(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_get_multiple_fields
        test_get_multiple_fields(**self.SHARED_CASE_KWARGS)

    def test_remove_field(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_remove_field
        test_remove_field(**self.SHARED_CASE_KWARGS)

    def test_remove_field_multi_selectors(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_remove_field_multi_selectors
        test_remove_field_multi_selectors(**self.SHARED_CASE_KWARGS)

    def test_remove_multiple_fields(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_remove_multiple_fields
        test_remove_multiple_fields(**self.SHARED_CASE_KWARGS)

    def test_update_field_return_old(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_update_field_return_old
        test_update_field_return_old(**self.SHARED_CASE_KWARGS)
