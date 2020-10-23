# StructNoSQL
Structured document based NoSQL client for DynamoDB with automatic data validation and advanced database queries functions.

### Installation (Python 3.6+ is required) :

```
pip install StructNoSQL
```

Example :

```python

# We define a field_type for each field. This is what create the basic validation
# tests, while there is also the possibility to make more complex tests.
class UsersTableModel(TableDataModel):
    userId = BaseField(name="userId", field_type=str)
    username = BaseField(name="username", field_type=str)
    moneyBalance = BaseField(name="moneyBalance", field_type=float)
    class ShoppingCartItem(MapModel):
        itemId = BaseField(name="itemId", field_type=str)
        addedToCartTimestamp = BaseField(name="timestamp", field_type=int)
    shoppingCartItems = BaseField(name="shoppingCartItems", field_type=Dict[str, ShoppingCartItem])
    # The validation tests will work on complex data structures. Like a list or a dict of items.

class UsersTable(BaseTable):
    def __init__(self):
        primary_index = PrimaryIndex(hash_key_name="userId", hash_key_variable_python_type=str)
        super().__init__(table_name="accounts-data", region_name="eu-west-2", 
                         data_model=UsersTableModel(), primary_index=primary_index, auto_create_table=True)


users_table = UsersTable()
shopping_cart_items = users_table.model.shoppingCartItems.query(key_name="userId", key_value="42").first_value()
# Will return in the form of a dict, the shopping cart items that passed the validation tests.
for item_key, item in shopping_cart_items.items():
    print(item.itemId)
    print(item.addedToCartTimestamp)
    # The variables of an item can be accessed as properties, and if they have
    # not been received in the response, you will not get any errors if you
    # try to access them or even if you perform operations on them.

```


## API Summary

### Table :

[BaseTable](#BaseTable)

[PrimaryIndex](#PrimaryIndex)

[GlobalSecondaryIndex](#GlobalSecondaryIndex)

### Data Models :
[TableDataModel](#TableDataModel)

[MapModel](#MapModel)

### Fields :

[BaseField](#BaseField)

[MapField](#MapField)

### Operations :

[put_record](#put_record)

[delete_record](#delete_record)

[get_single_field_item_from_single_item](#get_single_field_item_from_single_item)

[get_single_field_value_from_single_item](#get_single_field_value_from_single_item)

[get_multiple_fields_items_from_single_item](#get_multiple_fields_items_from_single_item)

[get_multiple_fields_values_from_single_item](#get_multiple_fields_values_from_single_item)

[set_update_one_field](#set_update_one_field)

[set_update_multiple_fields](#set_update_multiple_fields)

[remove_single_item_at_path_target](#remove_single_item_at_path_target)

[remove_multiple_items_at_path_targets](#remove_multiple_items_at_path_targets)

### Operations constructor objects

[FieldGetter](#FieldGetter)

[FieldSetter](#FieldSetter)

## API Reference

### `TableDataModel`

Class to inherit from to define the DataModel of a table.

#### Example
```python
from StructNoSQL import TableDataModel, BaseField

class UsersTableModel(TableDataModel):
    accountId = BaseField(name="accountId", field_type=str, required=True)
    username = BaseField(name="username", field_type=str, required=True)
```

### `BaseField`
Primary object to declare the fields of your table, or fields of items in maps and lists.

#### Arguments

 - `name`: The name that your field will have in your database, in the data gotten from the database, in your target 
 paths, and that will be expected in set/update operations to the database. You are not required to have the same name
 for the name parameter and the variable name to which you assigned the BaseField object, yet, we encourage it for code 
 visibility.
    - type: `str`
    - required: `True`
 - `field_type`: 
    - type: `str|float|int|NoneType|Any|MapModel|list|dict|Dict[type, MapModel]|[type1, type2, ...]|(type1, type2, ...)`
    - required: `False`
    - default: `Any`
    - Examples :
      ```python
      from StructNoSQL import TableDataModel, BaseField, MapModel, NoneType
      from typing import Dict
      
      class AccountsTableModel(TableDataModel):
          accountId = BaseField(name="accountId", field_type=str)
          class ProjectModel(MapModel):
              projectName = BaseField(name="projectName", field_type=str)
          projects = BaseField(name="projects", field_type=Dict[str, ProjectModel], key_name="projectId")
          activePromoCode = BaseField(name="activePromoCode", field_type=[str, NoneType])
      ```
 - `required`: If a required field is missing from any data (both when setting/updating data, and when retrieving it), 
 in a table model or in a MapModel, the data validation will print an error (without raising an exception), and not 
 insert or include the data in the response. If some items in a list or map fails the data validation, only the specific 
 items that fails will not be inserted/not included in the response; the other items that passed the data validation 
 will be included.
    - type: `bool`
    - required: `False`
    - default: `False`
    - Example :
      ```python
      from StructNoSQL import TableDataModel, BaseField
      
      class AccountsTableModel(TableDataModel):
          accountId = BaseField(name="accountId", field_type=str, required=True)
      ```
 - `not_modifiable`: Act as a read-only parameter. Once a field_value that is not_modifiable is set, trying to change
 or update its value will cause a rejection from the data validation (without raising an exception). You might use
 this parameter for your id's fields.
    - type: `bool`
    - required: `False`
    - default: `False`
    - Example :
      ```python
      from StructNoSQL import TableDataModel, BaseField
      
      class AccountsTableModel(TableDataModel):
          accountId = BaseField(name="accountId", field_type=str, not_modifiable=True)
      ```
 - `custom_default_value`: When you insert/update an item model, all the fields that were not required and that you did
 not specify will initialize themselves with the field_type that you precised (or the first acceptable field_type if
 you have a list or tuple of types as the field_type, or the primitive field_type of an object. [Learn more about 
 objects primitive types](#Objects-to-primitives-types-)). By specifying a custom_default_value, you can override this
 behavior just explained, and bring your own default_values to the initialization of a field.
    - type: `Any`
    - required: `False`
    - default: `None`
    - Example :
      ```python
      from StructNoSQL import TableDataModel, BaseField
      
      class AccountsTableModel(TableDataModel):
          activePromoCode = BaseField(name="activePromoCode", field_type=str, custom_default_value="signupPromoCode")
      ```
 - `key_name`: Reserved for BaseField's with field_type Dict[str, type]. Allows you to define a specific key that will
 be used in your database_path for navigating inside an item in your field, instead of the default f"{field.name}key".
 So, without specifying a key_name on our projects field below, the key_name in the database_path to navigate into
 items of the dict, would be projectsKey (the name of the field, with 'Key' appended to it). By defining our own 
 key_name, we can customize the key to something more fitting to what the key represent, for example : 'projectId'.
    - type: `str`
    - required: `False`
    - default: `None`
    - Example :
      ```python
      from StructNoSQL import TableDataModel, BaseField, MapModel
      from typing import Dict
      
      class AccountsTableModel(TableDataModel):
          class ProjectModel(MapModel):
              projectName = BaseField(name="projectName", field_type=str)
          projects = BaseField(name="projects", field_type=Dict[str, ProjectModel], key_name="projectId")
      ```
 
 ### Various infos about the inner-working of StructNoSQL
 
 #### Objects to primitives types :
- MapModel : dict
- Dict: dict
- List: list

 #### Conversion of floats to Decimals :

