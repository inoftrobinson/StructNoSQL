import unittest

from tests.components.playground_table_clients import PlaygroundInoftVocalEngineCachingTable
from tests.tests_remove_record.table_models import InoftVocalEngineTableModel


class TestsInoftVocalEngineCachingTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = PlaygroundInoftVocalEngineCachingTable(data_model=InoftVocalEngineTableModel)
        self.users_table.debug = True

        self.SHARED_CASE_KWARGS = {'self': self, 'users_table': self.users_table, 'primary_key_name': 'accountProjectUserId', 'is_caching': True}

    def test_basic_record_removal(self):
        from tests.tests_remove_record.cases_shared import test_basic_record_removal
        test_basic_record_removal(**self.SHARED_CASE_KWARGS)
