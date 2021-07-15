import unittest

from tests.components.playground_table_clients import PlaygroundDynamoDBCachingTable
from tests.tests_data_retrieval_validation.table_models import DynamoDBFirstTableModel, DynamoDBSecondTableModel


class TestsDynamoDBCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.first_table = PlaygroundDynamoDBCachingTable(data_model=DynamoDBFirstTableModel)
        self.first_table.debug = True
        self.second_table = PlaygroundDynamoDBCachingTable(data_model=DynamoDBSecondTableModel)
        self.second_table.debug = True

        self.SHARED_CASE_KWARGS = {
            'self': self, 'first_table': self.first_table, 'second_table': self.second_table,
            'is_caching': True, 'primary_key_name': 'accountId'
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

    def test_grouped_remove_multiple_fields(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_grouped_remove_multiple_fields
        test_grouped_remove_multiple_fields(**self.SHARED_CASE_KWARGS)

    def test_update_field_return_old(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_update_field_return_old
        test_update_field_return_old(**self.SHARED_CASE_KWARGS)

    def test_update_multiple_fields_return_old(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_update_multiple_fields_return_old
        test_update_multiple_fields_return_old(**self.SHARED_CASE_KWARGS)

    def test_query_field(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_query_field
        test_query_field(**self.SHARED_CASE_KWARGS)

    def test_query_field_multi_selectors(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_query_field_multi_selectors
        test_query_field_multi_selectors(**self.SHARED_CASE_KWARGS)

    def test_query_multiple_fields(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_query_multiple_fields
        test_query_multiple_fields(**self.SHARED_CASE_KWARGS)

    def test_remove_record(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_remove_record
        test_remove_record(**self.SHARED_CASE_KWARGS)
