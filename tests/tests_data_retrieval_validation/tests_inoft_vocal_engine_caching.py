import unittest

from tests.components.playground_table_clients import PlaygroundInoftVocalEngineCachingTable
from tests.tests_data_retrieval_validation.table_models import InoftVocalEngineFirstTableModel, InoftVocalEngineSecondTableModel


class TestsInoftVocalEngineCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.first_table = PlaygroundInoftVocalEngineCachingTable(data_model=InoftVocalEngineFirstTableModel)
        self.second_table = PlaygroundInoftVocalEngineCachingTable(data_model=InoftVocalEngineSecondTableModel)

        self.SHARED_CASE_KWARGS = {'self': self, 'first_table': self.first_table, 'second_table': self.second_table}

    def test_get_field(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_get_field
        test_get_field(**self.SHARED_CASE_KWARGS)

    def test_get_field_multi_selectors(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_get_field_multi_selectors
        test_get_field_multi_selectors(**self.SHARED_CASE_KWARGS)

    def test_get_multiple_fields(self):
        from tests.tests_data_retrieval_validation.cases_shared import test_get_multiple_fields
        test_get_multiple_fields(**self.SHARED_CASE_KWARGS)
