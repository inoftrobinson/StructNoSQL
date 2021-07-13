import unittest

from tests.components.playground_table_clients import PlaygroundInoftVocalEngineCachingTable
from tests.tests_update_field_return_old.table_models import InoftVocalEngineTableModel


class TestsInoftVocalEngineBasicTable(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        # self.users_table = PlaygroundInoftVocalEngineBasicTable(data_model=InoftVocalEngineTableModel)  # todo: implement the InoftVocalEngineBasicTable
        self.users_table = PlaygroundInoftVocalEngineCachingTable(data_model=InoftVocalEngineTableModel)

        self.SHARED_CASE_KWARGS = {'self': self, 'users_table': self.users_table, 'primary_key_name': 'accountProjectUserId', 'is_caching': False}

    def test_update_field_return_old(self):
        pass
        """from tests.tests_update_field_return_old.cases_shared import test_update_field_return_old
        test_update_field_return_old(**self.SHARED_CASE_KWARGS)"""
