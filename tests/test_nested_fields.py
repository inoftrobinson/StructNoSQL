import unittest
from typing import Dict
from StructNoSQL import BaseField, MapModel, TableDataModel, PrimaryIndex, GlobalSecondaryIndex, ActiveSelf, \
    DynamoDBBasicTable


class NestedFieldTableModel(TableDataModel):
    accountId = BaseField(name="accountId", field_type=str, required=True)
    class PropertyModel(MapModel):
        name = BaseField(name="name", field_type=str)
        childProperties = BaseField(name="childProperties", field_type=Dict[str, ActiveSelf], key_name="childPropertyKey{i}", required=False)
    properties = BaseField(name="properties", field_type=Dict[str, PropertyModel], key_name="propertyKey", required=False)


class Table(DynamoDBBasicTable):
    def __init__(self):
        primary_index = PrimaryIndex(hash_key_name="accountId", hash_key_variable_python_type=str)
        globals_secondary_indexes = [
            GlobalSecondaryIndex(hash_key_name="username", hash_key_variable_python_type=str, projection_type="ALL"),
            GlobalSecondaryIndex(hash_key_name="email", hash_key_variable_python_type=str, projection_type="ALL"),
        ]
        super().__init__(
            table_name="structnosql-playground", region_name="eu-west-2",
            data_model=NestedFieldTableModel(),
            primary_index=primary_index, global_secondary_indexes=globals_secondary_indexes,
            auto_create_table=True
        )


class TestTableOperations(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)
        self.users_table = Table()

    def test_get_all_projects(self):
        print(self.users_table.fields_switch)
        for field in self.users_table.fields_switch.keys():
            print(field)

        """success_put = self.users_table.put_record({
            'accountId': 'staticId', 'properties': {}
        })
        self.assertTrue(success_put)"""

        success_update = self.users_table.update_field(
            index_name='accountId', key_value='staticId',
            field_path='properties.{{propertyKey}}.childProperties.{{childPropertyKey0}}.childProperties.{{childPropertyKey1}}.name',
            value_to_set="theGreatCode", query_kwargs={
                'propertyKey': 'staticPropertyKey',
                'childPropertyKey0': 'staticChildPropertyKey0',
                'childPropertyKey1': 'staticChildPropertyKey1'
            }
        )
        self.assertTrue(success_update)


if __name__ == '__main__':
    unittest.main()
