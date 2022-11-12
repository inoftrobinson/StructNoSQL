import unittest
from typing import Any, Dict
from uuid import uuid4

from StructNoSQL import FieldGetter, TableDataModel, BaseField
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, PROD_ACCOUNT_ID, PROD_PROJECT_ID
from tests.components.prod_inoft_vocal_engine_table_clients import ProdInoftVocalEngineTableBasicClient


class DynamoDBTableModel(TableDataModel):
    accountProjectTableKeyId = BaseField(field_type=str, required=True)
    simpleField = BaseField(field_type=str, required=False)


class TestsDynamoDBBasicTable(unittest.TestCase):
    def test_put_record(self):
        from tests.test_auto_leading_keys.cases_shared import test_put_record
        test_put_record(self, lambda data_model, auto_leading_key: ProdInoftVocalEngineTableBasicClient(
            data_model=data_model, auto_leading_key=auto_leading_key
        ))

    def test_get_field(self):
        from tests.test_auto_leading_keys.cases_shared import test_get_field
        test_get_field(self, lambda data_model, auto_leading_key: ProdInoftVocalEngineTableBasicClient(
            data_model=data_model, auto_leading_key=auto_leading_key
        ))

    def test_get_multiple_fields(self):
        from tests.test_auto_leading_keys.cases_shared import test_get_multiple_fields
        test_get_multiple_fields(self, lambda data_model, auto_leading_key: ProdInoftVocalEngineTableBasicClient(
            data_model=data_model, auto_leading_key=auto_leading_key
        ))

