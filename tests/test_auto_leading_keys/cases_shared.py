import logging
import unittest
from typing import Union, Dict, Any, Callable, Type, Optional
from uuid import uuid4

from StructNoSQL import DynamoDBBasicTable, DynamoDBCachingTable, TableDataModel, BaseField, FieldGetter
from tests.components.playground_table_clients import PROD_ACCOUNT_ID, PROD_PROJECT_ID


class DynamoDBTableModel(TableDataModel):
    accountProjectTableKeyId = BaseField(field_type=str, required=True)
    simpleField = BaseField(field_type=str, required=False)


def test_put_record(self: unittest.TestCase, table_client_factory: Callable[[Type[TableDataModel], Optional[str]], Union[DynamoDBBasicTable, DynamoDBCachingTable]]):
    auto_leading_key: str = f"{PROD_ACCOUNT_ID}-{PROD_PROJECT_ID}-table1-"
    table_client = table_client_factory(DynamoDBTableModel, auto_leading_key)

    random_field_value = f"randomFieldValue_{uuid4()}"

    put_success: bool = table_client.put_record({
        'accountProjectTableKeyId': "exampleRecordKey",
        'simpleField': random_field_value
    })
    self.assertTrue(put_success)

    retrieved_values: Dict[str, Any] = table_client.get_field(
        key_value='exampleRecordKey', field_path='(accountProjectTableKeyId, simpleField)'
    )
    self.assertEqual(retrieved_values, {
        'accountProjectTableKeyId': "exampleRecordKey",
        'simpleField': random_field_value
    })

def test_get_field(self: unittest.TestCase, table_client_factory: Callable[[Type[TableDataModel], Optional[str]], Union[DynamoDBBasicTable, DynamoDBCachingTable]]):
    auto_leading_key: str = f"{PROD_ACCOUNT_ID}-{PROD_PROJECT_ID}-table1-"
    table_client_with_auto_leading_key = table_client_factory(DynamoDBTableModel, auto_leading_key)
    random_field_value: str = f"simpleField_{uuid4()}"

    put_success: bool = table_client_with_auto_leading_key.put_record({
        'accountProjectTableKeyId': "exampleRecordKey",
        'simpleField': random_field_value
    })
    self.assertTrue(put_success)

    retrieved_record_key_with_auto_leading_key: Any = table_client_with_auto_leading_key.get_field(
        key_value="exampleRecordKey", field_path='accountProjectTableKeyId'
    )
    self.assertEqual("exampleRecordKey", retrieved_record_key_with_auto_leading_key)

    retrieved_simple_field_with_auto_leading_key: Any = table_client_with_auto_leading_key.get_field(
        key_value="exampleRecordKey", field_path='simpleField'
    )
    self.assertEqual(random_field_value, retrieved_simple_field_with_auto_leading_key)

    table_client_without_auto_leading_key = table_client_factory(DynamoDBTableModel, None)

    retrieved_record_key_with_auto_leading_key_disabled: Optional[Any] = table_client_without_auto_leading_key.get_field(
        key_value=f"{auto_leading_key}exampleRecordKey", field_path='accountProjectTableKeyId'
    )
    self.assertEqual(f"{auto_leading_key}exampleRecordKey", retrieved_record_key_with_auto_leading_key_disabled)

    retrieved_simple_field_with_auto_leading_key_disabled: Optional[Any] = table_client_without_auto_leading_key.get_field(
        key_value=f"{auto_leading_key}exampleRecordKey", field_path='simpleField'
    )
    self.assertEqual(random_field_value, retrieved_simple_field_with_auto_leading_key_disabled)


def test_get_field_multi_selectors(self: unittest.TestCase, table_client_factory: Callable[[Type[TableDataModel], Optional[str]], Union[DynamoDBBasicTable, DynamoDBCachingTable]]):
    auto_leading_key: str = f"{PROD_ACCOUNT_ID}-{PROD_PROJECT_ID}-table1-"
    table_client_with_auto_leading_key = table_client_factory(DynamoDBTableModel, auto_leading_key)

    random_record_key: str = f"randomRecordKey_testGetFieldMultiSelectors_{uuid4()}"
    random_field_value: str = f"simpleField_{uuid4()}"

    put_success: bool = table_client_with_auto_leading_key.put_record({
        'accountProjectTableKeyId': random_record_key,
        'simpleField': random_field_value
    })
    self.assertTrue(put_success)

    retrieved_fields_with_auto_leading_key: Any = table_client_with_auto_leading_key.get_field(
        key_value=random_record_key, field_path='(accountProjectTableKeyId,simpleField)'
    )
    self.assertEqual({
        'accountProjectTableKeyId': random_record_key,
        'simpleField': random_field_value
    }, retrieved_fields_with_auto_leading_key)

    table_client_without_auto_leading_key = table_client_factory(DynamoDBTableModel, None)

    retrieved_fields_with_auto_leading_key_disabled_and_filled_key_value: Optional[Any] = table_client_without_auto_leading_key.get_field(
        key_value=f"{auto_leading_key}{random_record_key}", field_path='(accountProjectTableKeyId,simpleField)'
    )
    self.assertEqual({
        'accountProjectTableKeyId': f"{auto_leading_key}{random_record_key}",
        'simpleField': random_field_value
    }, retrieved_fields_with_auto_leading_key_disabled_and_filled_key_value)

    retrieved_fields_with_auto_leading_key_disabled: Optional[Any] = table_client_without_auto_leading_key.get_field(
        key_value=random_record_key, field_path='(accountProjectTableKeyId,simpleField)'
    )
    self.assertEqual({
        'accountProjectTableKeyId': None,
        'simpleField': None
    }, retrieved_fields_with_auto_leading_key_disabled)


def test_get_multiple_fields(self: unittest.TestCase, table_client_factory: Callable[[Type[TableDataModel], Optional[str]], Union[DynamoDBBasicTable, DynamoDBCachingTable]]):
    random_record_key: str = f"randomRecordKey_{uuid4()}"
    random_field_value = f"randomFieldValue_{uuid4()}"
    auto_leading_key: str = f"{PROD_ACCOUNT_ID}-{PROD_PROJECT_ID}-table1-"

    table_client = table_client_factory(DynamoDBTableModel, auto_leading_key)

    update_success: bool = table_client.update_field(
        key_value=random_record_key,
        field_path="simpleField",
        value_to_set=random_field_value
    )
    self.assertTrue(update_success)

    retrieved_items_with_auto_leading_key: Dict[str, Any] = (
        table_client.get_multiple_fields(key_value=random_record_key, getters={
            'recordKey': FieldGetter(field_path='accountProjectTableKeyId'),
            'field': FieldGetter(field_path="simpleField")
        })
    )
    self.assertEqual({
        'recordKey': random_record_key,
        'field': random_field_value
    }, retrieved_items_with_auto_leading_key)

    table_client_without_auto_leading_key = table_client_factory(DynamoDBTableModel, None)

    retrieved_items_with_auto_leading_key_disabled: Dict[str, Any] = (
        table_client_without_auto_leading_key.get_multiple_fields(key_value=random_record_key, getters={
            'recordKey': FieldGetter(field_path='accountProjectTableKeyId'),
            'field': FieldGetter(field_path="simpleField")
        })
    )
    self.assertEqual({
        'recordKey': None,
        'field': None
    }, retrieved_items_with_auto_leading_key_disabled)


def test_update_field(self: unittest.TestCase, table_client_factory: Callable[[Type[TableDataModel], Optional[str]], Union[DynamoDBBasicTable, DynamoDBCachingTable]]):
    random_record_key: str = f"randomRecordKey_{uuid4()}"
    random_field_value = f"randomFieldValue_{uuid4()}"
    auto_leading_key: str = f"{PROD_ACCOUNT_ID}-{PROD_PROJECT_ID}-table1-"

    table_client = table_client_factory(DynamoDBTableModel, auto_leading_key)

    update_success: bool = table_client.update_field(
        key_value=random_record_key,
        field_path="simpleField",
        value_to_set=random_field_value
    )
    self.assertTrue(update_success)

    table_client_without_auto_leading_key = table_client_factory(DynamoDBTableModel, None)

    random_field_value_without_auto_leading_key: str = f"randomFieldValueWithoutAutoLeadingKey_{uuid4()}"
    update_on_field_with_leading_key: bool = table_client_without_auto_leading_key.update_field(
        key_value=random_record_key, field_path="simpleField", value_to_set=random_field_value_without_auto_leading_key
    )
    self.assertTrue(update_on_field_with_leading_key)

    retrieved_field_value_after_update_without_leading_key: str = table_client.get_field(
        key_value=random_record_key, field_path="simpleField"
    )
    self.assertEqual(random_field_value, retrieved_field_value_after_update_without_leading_key)



