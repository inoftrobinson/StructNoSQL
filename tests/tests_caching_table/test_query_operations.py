import random
import unittest
from uuid import uuid4
from typing import Dict, Any

from StructNoSQL import BaseField, TableDataModel, MapModel, FieldSetter, FieldGetter
from tests.tests_caching_table.caching_users_table import CachingUsersTable, TEST_ACCOUNT_ID, TEST_ACCOUNT_USERNAME

class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    simpleValue = BaseField(name='simpleValue', field_type=int, required=False)
    simpleValue2 = BaseField(name='simpleValue2', field_type=int, required=False)
    class ContainerModel(MapModel):
        fieldOne = BaseField(name='fieldOne', field_type=str, required=False)
        fieldTwo = BaseField(name='fieldTwo', field_type=str, required=False)
        fieldThree = BaseField(name='fieldThree', field_type=str, required=False)
    container = BaseField(name='container', field_type=Dict[str, ContainerModel], key_name='containerKey', required=False)


class TestQueryOperations(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = CachingUsersTable(data_model=TableModel)
        self.users_table.debug = True

    def test_set_get_fields_with_primary_index(self):
        self.users_table.clear_cached_data_and_pending_operations()
        random_field_value_one = random.randint(0, 100)
        random_field_value_two = random.randint(100, 200)

        set_update_success: bool = self.users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='simpleValue', value_to_set=random_field_value_one),
            FieldSetter(field_path='simpleValue2', value_to_set=random_field_value_two)
        ])
        self.assertTrue(set_update_success)

        update_commit_success: bool = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        retrieved_values = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='(simpleValue, simpleValue2)')
        self.assertEqual(retrieved_values, {
            'simpleValue': {'fromCache': True, 'value': random_field_value_one},
            'simpleValue2': {'fromCache': True, 'value': random_field_value_two}
        })

        single_field_not_primary_key = self.users_table.query_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual({TEST_ACCOUNT_ID: {'fromCache': False, 'value': random_field_value_one}}, single_field_not_primary_key)

        single_field_primary_key = self.users_table.query_field(key_value=TEST_ACCOUNT_ID, field_path='accountId')
        self.assertEqual({TEST_ACCOUNT_ID: {'fromCache': False, 'value': TEST_ACCOUNT_ID}}, single_field_primary_key)

        multiple_fields_without_primary_key = self.users_table.query_field(
            key_value=TEST_ACCOUNT_ID, field_path='(simpleValue, simpleValue2)'
        )
        self.assertEqual({
            TEST_ACCOUNT_ID: {
                'simpleValue': {'fromCache': False, 'value': random_field_value_one},
                'simpleValue2': {'fromCache': False, 'value': random_field_value_two}
            }}, multiple_fields_without_primary_key
        )

        multiple_fields_with_primary_key = self.users_table.query_field(
            key_value=TEST_ACCOUNT_ID, field_path='(accountId, simpleValue, simpleValue2)'
        )
        self.assertEqual({
            TEST_ACCOUNT_ID: {
                'accountId': {'fromCache': False, 'value': TEST_ACCOUNT_ID},
                'simpleValue': {'fromCache': False, 'value': random_field_value_one},
                'simpleValue2': {'fromCache': False, 'value': random_field_value_two}
            }}, multiple_fields_with_primary_key
        )

    def test_set_get_fields_with_secondary_index(self):
        self.users_table.clear_cached_data_and_pending_operations()
        random_field_value_one = random.randint(0, 100)
        random_field_value_two = random.randint(100, 200)

        set_update_success: bool = self.users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='simpleValue', value_to_set=random_field_value_one),
            FieldSetter(field_path='simpleValue2', value_to_set=random_field_value_two)
        ])
        self.assertTrue(set_update_success)

        update_commit_success: bool = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        retrieved_values = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='(simpleValue, simpleValue2)')
        self.assertEqual(retrieved_values, {
            'simpleValue': {'fromCache': True, 'value': random_field_value_one},
            'simpleValue2': {'fromCache': True, 'value': random_field_value_two}
        })

        single_field_not_primary_key = self.users_table.query_field(key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='simpleValue')
        self.assertEqual({TEST_ACCOUNT_ID: {'fromCache': False, 'value': random_field_value_one}}, single_field_not_primary_key)

        single_field_primary_key = self.users_table.query_field(key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='accountId')
        self.assertEqual({TEST_ACCOUNT_ID: {'fromCache': False, 'value': TEST_ACCOUNT_ID}}, single_field_primary_key)

        multiple_fields_without_primary_key = self.users_table.query_field(
            key_value=TEST_ACCOUNT_USERNAME, index_name='username', field_path='(simpleValue, simpleValue2)'
        )
        self.assertEqual({
            TEST_ACCOUNT_ID: {
                'simpleValue': {'fromCache': False, 'value': random_field_value_one},
                'simpleValue2': {'fromCache': False, 'value': random_field_value_two}
            }}, multiple_fields_without_primary_key
        )

        multiple_fields_with_primary_key = self.users_table.query_field(
            key_value=TEST_ACCOUNT_USERNAME, index_name='username',
            field_path='(accountId, simpleValue, simpleValue2)'
        )
        self.assertEqual({
            TEST_ACCOUNT_ID: {
                'accountId': {'fromCache': False, 'value': TEST_ACCOUNT_ID},
                'simpleValue': {'fromCache': False, 'value': random_field_value_one},
                'simpleValue2': {'fromCache': False, 'value': random_field_value_two}
            }}, multiple_fields_with_primary_key
        )

    def test_set_get_fields_with_overriding_names(self):
        item1_field1_random_value: str = f"item1_field1_${uuid4()}"
        item2_field1_random_value: str = f"item2_field1_${uuid4()}"
        set_update_success: bool = self.users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(
                field_path='container.{{containerKey}}',
                query_kwargs={'containerKey': "item1"},
                value_to_set={'fieldOne': item1_field1_random_value}
            ),
            FieldSetter(
                field_path='container.{{containerKey}}',
                query_kwargs={'containerKey': "item2"},
                value_to_set={'fieldOne': item2_field1_random_value}
            )
        ])
        self.assertTrue(set_update_success)

        commit_success: bool = self.users_table.commit_operations()
        self.assertTrue(commit_success)

        retrieved_items_values: Dict[str, Any] = self.users_table.query_multiple_fields(
            key_value=TEST_ACCOUNT_USERNAME, index_name='username', getters={
                'item1-value': FieldGetter(field_path='container.{{containerKey}}.fieldOne', query_kwargs={'containerKey': "item1"}),
                'item2-value': FieldGetter(field_path='container.{{containerKey}}.fieldOne', query_kwargs={'containerKey': "item2"}),
            }
        )
        self.assertEqual({
            TEST_ACCOUNT_ID: {
                'item1-value': {'fromCache': False, 'value': item1_field1_random_value},
                'item2-value': {'fromCache': False, 'value': item2_field1_random_value}
            }}, retrieved_items_values
        )


if __name__ == '__main__':
    unittest.main()
