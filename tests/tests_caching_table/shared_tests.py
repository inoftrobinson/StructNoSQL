import random
import unittest
from abc import abstractmethod
from typing import List, Callable
from uuid import uuid4
from StructNoSQL import TableDataModel, BaseField, FieldRemover, MapModel, FieldSetter, FieldGetter
from StructNoSQL.tables.dynamodb_caching_table import DynamoDBCachingTable
from StructNoSQL.tables.inoft_vocal_engine_caching_table import InoftVocalEngineCachingTable
from tests.tests_caching_table.caching_users_table import CachingUsersTable, TEST_ACCOUNT_ID


# todo: add an unit test that make sure that what matter with the field are the field names, not their variable names

class TableModel(TableDataModel):
    accountId = BaseField(name='accountId', field_type=str, required=True)
    simpleValue = BaseField(name='simpleValue', field_type=int, required=False)
    simpleValue2 = BaseField(name='simpleValue2', field_type=int, required=False)
    fieldToDelete = BaseField(name='fieldToDelete', field_type=int, required=False)
    fieldToDelete2 = BaseField(name='fieldToDelete2', field_type=int, required=False)
    fieldToRemove = BaseField(name='fieldToRemove', field_type=int, required=False)
    fieldToRemove2 = BaseField(name='fieldToRemove2', field_type=int, required=False)
    class ContainerToRemoveModel(MapModel):
        fieldOne = BaseField(name='fieldOne', field_type=str, required=False)
        fieldTwo = BaseField(name='fieldTwo', field_type=str, required=False)
        fieldThree = BaseField(name='fieldThree', field_type=str, required=False)
    containerToRemove = BaseField(name='containerToRemove', field_type=ContainerToRemoveModel, required=False)
    containersListToRemove = BaseField(name='containersListToRemove', field_type=List[ContainerToRemoveModel], key_name='listIndex')

class TestsSharedAcrossDynamoDBAndInoftVocalEngine(unittest.TestCase):
    def __init__(self, method_name: str, table_factory: Callable[[], DynamoDBCachingTable or InoftVocalEngineCachingTable]):
        super().__init__(methodName=method_name)
        self.table_factory = table_factory

    def reset_table(self):
        self.users_table: DynamoDBCachingTable or InoftVocalEngineCachingTable = self.table_factory()
        self.users_table.debug = True

    def test_simple_get_field(self):
        self.reset_table()

        first_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual(first_response_data['fromCache'], False)

        second_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual(second_response_data['fromCache'], True)

    def test_set_then_get_field_from_cache(self):
        self.reset_table()
        random_field_value = random.randint(0, 100)

        update_success = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue', value_to_set=random_field_value)
        self.assertTrue(update_success)

        retrieve_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual(retrieve_response_data['fromCache'], True)
        
    def test_set_then_get_multiple_fields(self):
        self.reset_table()
        random_field_one_value = random.randint(0, 99)
        random_field_two_value = random.randint(100, 199)

        update_success = self.users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='simpleValue', value_to_set=random_field_one_value),
            FieldSetter(field_path='simpleValue2', value_to_set=random_field_two_value)
        ])
        self.assertTrue(update_success)

        retrieve_response_data = self.users_table.get_multiple_fields(key_value=TEST_ACCOUNT_ID, getters={
            'one': FieldGetter(field_path='simpleValue'),
            'two': FieldGetter(field_path='simpleValue2')
        })
        self.assertIsNone(retrieve_response_data['fromCache'])
        self.assertEqual(retrieve_response_data['value'].get('one', None), {'value': random_field_one_value, 'fromCache': True})
        self.assertEqual(retrieve_response_data['value'].get('two', None), {'value': random_field_two_value, 'fromCache': True})

    def test_set_then_get_pack_values_with_one_of_them_present_in_cache(self):
        self.reset_table()
        random_field_one_value = random.randint(0, 99)
        random_field_two_value = random.randint(100, 199)

        update_success = self.users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='simpleValue', value_to_set=random_field_one_value),
            FieldSetter(field_path='simpleValue2', value_to_set=random_field_two_value)
        ])
        self.assertTrue(update_success)

        self.users_table.commit_operations()
        self.reset_table()

        # Caching the simpleValue field
        first_retrieved_first_value = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual({'value': random_field_one_value, 'fromCache': False}, first_retrieved_first_value)

        # With get_field function and multi selector
        get_field_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='(simpleValue, simpleValue2)')
        self.assertIsNone(get_field_response_data['fromCache'])
        self.assertEqual({'value': random_field_one_value, 'fromCache': True}, get_field_response_data['value'].get('simpleValue', None))
        self.assertEqual({'value': random_field_two_value, 'fromCache': False}, get_field_response_data['value'].get('simpleValue2', None))

        self.reset_table()
        # Caching the simpleValue field
        second_retrieved_first_value = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertFalse(second_retrieved_first_value['fromCache'])
        self.assertEqual(second_retrieved_first_value['value'], random_field_one_value)

        # With get_multiple_fields function
        get_multiple_fields_response_data = self.users_table.get_multiple_fields(key_value=TEST_ACCOUNT_ID, getters={
            'one': FieldGetter(field_path='simpleValue'),
            'two': FieldGetter(field_path='simpleValue2')
        })
        self.assertIsNone(get_multiple_fields_response_data['fromCache'])
        self.assertEqual(get_multiple_fields_response_data['value'].get('one', None), {'value': random_field_one_value, 'fromCache': True})
        self.assertEqual(get_multiple_fields_response_data['value'].get('two', None), {'value': random_field_two_value, 'fromCache': False})

    def test_debug_simple_set_commit_then_get_field_from_database(self):
        self.reset_table()
        random_field_value = random.randint(0, 100)

        update_success = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue', value_to_set=random_field_value)
        self.assertTrue(update_success)
        commit_success = self.users_table.commit_operations()
        self.assertTrue(commit_success)

        self.reset_table()
        retrieve_response_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='simpleValue')
        self.assertEqual(retrieve_response_data['value'], random_field_value)
        self.assertEqual(retrieve_response_data['fromCache'], False)

    def test_update_multiple_fields(self):
        self.reset_table()
        random_field_one_value = random.randint(0, 99)
        random_field_two_value = random.randint(100, 199)

        update_success = self.users_table.update_multiple_fields(key_value=TEST_ACCOUNT_ID, setters=[
            FieldSetter(field_path='simpleValue', value_to_set=random_field_one_value),
            FieldSetter(field_path='simpleValue2', value_to_set=random_field_two_value)
        ])
        self.assertTrue(update_success)

        retrieved_data = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='(simpleValue, simpleValue2)')
        self.assertIsNone(retrieved_data['fromCache'])
        self.assertEqual(retrieved_data['value'].get('simpleValue', None), {'value': random_field_one_value, 'fromCache': True})
        self.assertEqual(retrieved_data['value'].get('simpleValue2', None), {'value': random_field_two_value, 'fromCache': True})

    def test_set_delete_field(self):
        self.reset_table()
        random_field_value = random.randint(0, 100)

        update_success = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete', value_to_set=random_field_value)
        self.assertTrue(update_success)
        update_commit_success = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        delete_success = self.users_table.delete_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete')
        self.assertTrue(delete_success)
        delete_commit_success = self.users_table.commit_operations()
        self.assertTrue(delete_commit_success)

        retrieved_expected_empty_value_from_cache = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete')
        self.assertTrue(retrieved_expected_empty_value_from_cache['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_from_cache['value'])

        self.reset_table()
        retrieved_expected_empty_value_from_database = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete')
        self.assertFalse(retrieved_expected_empty_value_from_database['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_from_database['value'])

    def test_set_remove_field(self):
        self.reset_table()
        random_field_value = random.randint(0, 100)

        update_success = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove', value_to_set=random_field_value)
        self.assertTrue(update_success)
        update_commit_success = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        removed_value = self.users_table.remove_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove')
        self.assertTrue(removed_value['fromCache'])
        self.assertEqual(removed_value['value'], random_field_value)

        retrieved_expected_empty_value_from_cache = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove')
        self.assertTrue(retrieved_expected_empty_value_from_cache['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_from_cache['value'])

        self.users_table.commit_operations()
        self.reset_table()

        retrieved_expected_empty_value_from_database = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove')
        self.assertFalse(retrieved_expected_empty_value_from_database['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_from_database['value'])

    def test_dict_data_unpacking(self):
        """
        Data unpacking handle the scenario where an object would be put in the cache (like a dictionary, that has been
        updated as an object, instead of doing it field per field), and then later on, we try to access from the cache
        one field of packed data. We want to be handle to retrieve data that was packed in bigger object, which means
        that we cannot use a simple flatten indexation of the inserted/updated data to then later access it in the cache.
        """
        self.reset_table()
        random_field_one_value = f"field_one_{uuid4()}"
        random_field_two_value = f"field_two_{uuid4()}"

        update_success = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='containerToRemove', value_to_set={
            'fieldOne': random_field_one_value, 'fieldTwo': random_field_two_value
        })
        self.assertTrue(update_success)
        update_commit_success = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        retrieved_value = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='containerToRemove.(fieldOne, fieldTwo)')
        self.assertIsNone(retrieved_value['fromCache'])
        self.assertEqual(retrieved_value['value'].get('fieldOne', {}), {'value': random_field_one_value, 'fromCache': True})
        self.assertEqual(retrieved_value['value'].get('fieldTwo', {}), {'value': random_field_two_value, 'fromCache': True})

        removed_value = self.users_table.remove_field(key_value=TEST_ACCOUNT_ID, field_path='containerToRemove.(fieldOne, fieldTwo)')
        self.assertIsNone(removed_value['fromCache'])
        self.assertEqual(removed_value['value'].get('fieldOne', {}), {'value': random_field_one_value, 'fromCache': True})
        self.assertEqual(removed_value['value'].get('fieldTwo', {}), {'value': random_field_two_value, 'fromCache': True})

    def test_list_data_unpacking(self):
        """
        Data unpacking handle the scenario where an object would be put in the cache (like a dictionary, that has been
        updated as an object, instead of doing it field per field), and then later on, we try to access from the cache
        one field of packed data. We want to be handle to retrieve data that was packed in bigger object, which means
        that we cannot use a simple flatten indexation of the inserted/updated data to then later access it in the cache.
        """
        self.reset_table()
        random_field_one_value = f"field_one_{uuid4()}"
        random_field_two_value = f"field_two_{uuid4()}"

        update_success = self.users_table.update_field(
            key_value=TEST_ACCOUNT_ID, field_path='containersListToRemove.{{listIndex}}', query_kwargs={'listIndex': 0},
            value_to_set={'fieldOne': random_field_one_value, 'fieldTwo': random_field_two_value}
        )
        self.assertTrue(update_success)
        update_commit_success = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        retrieved_value = self.users_table.get_field(
            key_value=TEST_ACCOUNT_ID, query_kwargs={'listIndex': 0},
            field_path='containersListToRemove.{{listIndex}}.(fieldOne, fieldTwo)'
        )
        self.assertIsNone(retrieved_value['fromCache'])
        self.assertEqual(retrieved_value['value'].get('fieldOne', {}), {'value': random_field_one_value, 'fromCache': True})
        self.assertEqual(retrieved_value['value'].get('fieldTwo', {}), {'value': random_field_two_value, 'fromCache': True})

        removed_value = self.users_table.remove_field(
            key_value=TEST_ACCOUNT_ID, query_kwargs={'listIndex': 0},
            field_path='containersListToRemove.{{listIndex}}.(fieldOne, fieldTwo)'
        )
        self.assertIsNone(removed_value['fromCache'])
        self.assertEqual(removed_value['value'].get('fieldOne', {}), {'value': random_field_one_value, 'fromCache': True})
        self.assertEqual(removed_value['value'].get('fieldTwo', {}), {'value': random_field_two_value, 'fromCache': True})

    def test_set_remove_multi_selector_field_and_field_unpacking(self):
        self.reset_table()
        random_field_one_value = f"field_one_{uuid4()}"
        random_field_two_value = f"field_two_{uuid4()}"
        random_field_three_value = f"field_three_{uuid4()}"

        update_success = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='containerToRemove', value_to_set={
            'fieldOne': random_field_one_value, 'fieldTwo': random_field_two_value, 'fieldThree': random_field_three_value
        })
        self.assertTrue(update_success)
        update_commit_success = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        removed_value = self.users_table.remove_field(key_value=TEST_ACCOUNT_ID, field_path='containerToRemove.(fieldOne, fieldThree)')
        self.assertIsNone(removed_value['fromCache'])
        self.assertEqual(removed_value['value'].get('fieldOne', {}), {'value': random_field_one_value, 'fromCache': True})
        self.assertEqual(removed_value['value'].get('fieldThree', {}), {'value': random_field_three_value, 'fromCache': True})

        self.reset_table()
        removed_value = self.users_table.remove_field(key_value=TEST_ACCOUNT_ID, field_path='containerToRemove.(fieldOne, fieldThree)')
        self.assertIsNone(removed_value['fromCache'])
        self.assertEqual({'value': random_field_one_value, 'fromCache': False}, removed_value['value'].get('fieldOne', {}))
        self.assertEqual({'value': random_field_three_value, 'fromCache': False}, removed_value['value'].get('fieldThree', {}))

    def test_set_delete_multiple_fields(self):
        self.reset_table()
        random_field_value_one = random.randint(0, 100)
        random_field_value_two = random.randint(100, 200)

        update_success_one = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete', value_to_set=random_field_value_one)
        self.assertTrue(update_success_one)
        update_success_two = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete2', value_to_set=random_field_value_two)
        self.assertTrue(update_success_two)
        update_commit_success = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        retrieved_data_one = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete')
        self.assertEqual(retrieved_data_one['value'], random_field_value_one)
        retrieved_data_two = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete2')
        self.assertEqual(retrieved_data_two['value'], random_field_value_two)

        multi_delete_response = self.users_table.delete_multiple_fields(
            key_value=TEST_ACCOUNT_ID, removers={
                'one': FieldRemover(field_path='fieldToDelete'),
                'two': FieldRemover(field_path='fieldToDelete2')
            }
        )
        self.assertTrue(all(multi_delete_response.values()))

        retrieved_expected_empty_value_one_from_cache = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete')
        self.assertTrue(retrieved_expected_empty_value_one_from_cache['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_one_from_cache['value'])
        retrieved_expected_empty_value_two_from_cache = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete2')
        self.assertTrue(retrieved_expected_empty_value_two_from_cache['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_two_from_cache['value'])

        self.users_table.commit_operations()
        self.reset_table()

        retrieved_expected_empty_value_one_from_database = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete')
        self.assertFalse(retrieved_expected_empty_value_one_from_database['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_one_from_database['value'])
        retrieved_expected_empty_value_two_from_database = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToDelete2')
        self.assertFalse(retrieved_expected_empty_value_two_from_database['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_two_from_database['value'])
        
    def test_set_remove_multiple_fields(self):
        self.reset_table()
        random_field_value_one = random.randint(0, 100)
        random_field_value_two = random.randint(100, 200)

        update_success_one = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove', value_to_set=random_field_value_one)
        self.assertTrue(update_success_one)
        update_success_two = self.users_table.update_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove2', value_to_set=random_field_value_two)
        self.assertTrue(update_success_two)
        update_commit_success = self.users_table.commit_operations()
        self.assertTrue(update_commit_success)

        retrieved_data_one = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove')
        self.assertEqual(retrieved_data_one['value'], random_field_value_one)
        retrieved_data_two = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove2')
        self.assertEqual(retrieved_data_two['value'], random_field_value_two)

        response_data = self.users_table.remove_multiple_fields(key_value=TEST_ACCOUNT_ID, removers={
            'one': FieldRemover(field_path='fieldToRemove'),
            'two': FieldRemover(field_path='fieldToRemove2')
        })
        self.assertEqual({'fromCache': True, 'value': random_field_value_one}, response_data.get('one', None))
        self.assertEqual({'fromCache': True, 'value': random_field_value_two}, response_data.get('two', None))

        retrieved_expected_empty_value_one_from_cache = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove')
        self.assertTrue(retrieved_expected_empty_value_one_from_cache['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_one_from_cache['value'])
        retrieved_expected_empty_value_two_from_cache = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove2')
        self.assertTrue(retrieved_expected_empty_value_two_from_cache['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_two_from_cache['value'])

        self.users_table.commit_operations()
        self.reset_table()

        retrieved_expected_empty_value_one_from_database = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove')
        self.assertFalse(retrieved_expected_empty_value_one_from_database['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_one_from_database['value'])
        retrieved_expected_empty_value_two_from_database = self.users_table.get_field(key_value=TEST_ACCOUNT_ID, field_path='fieldToRemove2')
        self.assertFalse(retrieved_expected_empty_value_two_from_database['fromCache'])
        self.assertIsNone(retrieved_expected_empty_value_two_from_database['value'])

