import unittest
from typing import Any, Dict
from uuid import uuid4

from StructNoSQL import FieldGetter, TableDataModel, BaseField
from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, PROD_ACCOUNT_ID, PROD_PROJECT_ID


class DynamoDBTableModel(TableDataModel):
    accountProjectTableKeyId = BaseField(field_type=str, required=True)
    simpleField = BaseField(field_type=str, required=False)


class TestsDynamoDBBasicTable(unittest.TestCase):
    def test_get_field(self):
        pass

    def test_get_multiple_fields(self):
        random_field_value = f"randomFieldValue_{uuid4()}"
        auto_leading_key: str = f"{PROD_ACCOUNT_ID}-{PROD_PROJECT_ID}-table1-"

        from tests.components.prod_inoft_vocal_engine_table_clients import ProdInoftVocalEngineTableBasicClient
        table_client = ProdInoftVocalEngineTableBasicClient(
            data_model=DynamoDBTableModel,
            auto_leading_key=auto_leading_key
        )

        update_success: bool = table_client.update_field(
            key_value="exampleRecordKey",
            field_path="simpleField",
            value_to_set=random_field_value
        )
        self.assertTrue(update_success)

        retrieved_items_with_auto_leading_key: Dict[str, Any] = (
            table_client.get_multiple_fields(key_value="exampleRecordKey", getters={
                'recordKey': FieldGetter(field_path='accountProjectTableKeyId'),
                'field': FieldGetter(field_path="simpleField")
            })
        )
        self.assertEqual(retrieved_items_with_auto_leading_key, {
            'recordKey': "exampleRecordKey",
            'field': random_field_value
        })

        table_client.auto_leading_key = None
        retrieved_items_with_auto_leading_key_disabled: Dict[str, Any] = (
            table_client.get_multiple_fields(key_value="exampleRecordKey", getters={
                'recordKey': FieldGetter(field_path='accountProjectTableKeyId'),
                'field': FieldGetter(field_path="simpleField")
            })
        )
        self.assertEqual(retrieved_items_with_auto_leading_key_disabled, {
            'recordKey': None,
            'field': None
        })

