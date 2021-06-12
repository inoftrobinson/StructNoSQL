import unittest

from tests.tests_caching_table.caching_users_table import CachingUsersTable, InoftVocalEngineUsersCachingTable
from tests.tests_caching_table.shared_cases import AbstractContainer, DynamoDBTableModel, InoftVocalEngineTableModel


# Yes. Do not argue. Any resistance will be futile. Inheritance in unittest's is the way.


class TestDynamoDBCachingTable(AbstractContainer.TestsSharedAcrossDynamoDBAndInoftVocalEngine):
    def __init__(self, method_name: str):
        def table_factory():
            return CachingUsersTable(data_model=DynamoDBTableModel)
        super().__init__(method_name=method_name, table_factory=table_factory)

    def test_set_remove_fields_with_secondary_index(self):
        super().test_set_remove_fields_with_secondary_index()

class TestInoftVocalEngineCachingTable(AbstractContainer.TestsSharedAcrossDynamoDBAndInoftVocalEngine):
    def __init__(self, method_name: str):
        def table_factory():
            return InoftVocalEngineUsersCachingTable(data_model=InoftVocalEngineTableModel)
        super().__init__(method_name=method_name, table_factory=table_factory)

    def test_set_remove_fields_with_secondary_index(self):
        super().test_set_remove_fields_with_secondary_index()


if __name__ == '__main__':
    unittest.main()
