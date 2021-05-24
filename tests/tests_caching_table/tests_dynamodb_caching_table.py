import unittest

from tests.tests_caching_table.caching_users_table import CachingUsersTable, InoftVocalEngineUsersCachingTable
from tests.tests_caching_table.shared_tests import TestsSharedAcrossDynamoDBAndInoftVocalEngine, TableModel


# Yes. Do not argue. Any resistance will be futile. Inheritance in unittest's is the way.


class TestDynamoDBCachingTable(TestsSharedAcrossDynamoDBAndInoftVocalEngine):
    def __init__(self, method_name: str):
        def table_factory():
            return CachingUsersTable(data_model=TableModel)
        super().__init__(method_name=method_name, table_factory=table_factory)

class TestInoftVocalEngineCachingTable(TestsSharedAcrossDynamoDBAndInoftVocalEngine):
    def __init__(self, method_name: str):
        def table_factory():
            return InoftVocalEngineUsersCachingTable(data_model=TableModel)
        super().__init__(method_name=method_name, table_factory=table_factory)


if __name__ == '__main__':
    unittest.main()
