# StructNoSQL
Structured document based NoSQL client for DynamoDB with automatic data validation and advanced database queries functions.

### Installation (Python 3.7 is recommanded. Python 3.6+ is required) :

```
 pip install inoftvocal
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
