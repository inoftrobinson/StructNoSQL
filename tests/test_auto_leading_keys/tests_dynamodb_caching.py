import unittest

from StructNoSQL import TableDataModel, BaseField
from tests.components.prod_inoft_vocal_engine_table_clients import ProdInoftVocalEngineTableCachingClient


class DynamoDBTableModel(TableDataModel):
    accountProjectTableKeyId = BaseField(field_type=str, required=True)
    simpleField = BaseField(field_type=str, required=False)


class TestsDynamoDBCachingTable(unittest.TestCase):
    def test_put_record(self):
        from tests.test_auto_leading_keys.cases_shared import test_put_record
        test_put_record(self, lambda data_model, auto_leading_key: ProdInoftVocalEngineTableCachingClient(
            data_model=data_model, auto_leading_key=auto_leading_key
        ))

    def test_get_field(self):
        from tests.test_auto_leading_keys.cases_shared import test_get_field
        test_get_field(self, lambda data_model, auto_leading_key: ProdInoftVocalEngineTableCachingClient(
            data_model=data_model, auto_leading_key=auto_leading_key
        ))

    def test_get_field_multi_selectors(self):
        from tests.test_auto_leading_keys.cases_shared import test_get_field_multi_selectors
        test_get_field_multi_selectors(self, lambda data_model, auto_leading_key: ProdInoftVocalEngineTableCachingClient(
            data_model=data_model, auto_leading_key=auto_leading_key
        ))

    def test_get_multiple_fields(self):
        from tests.test_auto_leading_keys.cases_shared import test_get_multiple_fields
        test_get_multiple_fields(self, lambda data_model, auto_leading_key: ProdInoftVocalEngineTableCachingClient(
            data_model=data_model, auto_leading_key=auto_leading_key
        ))

