import re
from typing import List, Optional, Any, Dict, _GenericAlias, Tuple
from StructNoSQL.dynamodb.models import DatabasePathElement
from StructNoSQL.exceptions import InvalidFieldNameException, UsageOfUntypedSetException
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.query import Query
from StructNoSQL.utils.types import ACCEPTABLE_KEY_TYPES

FIELD_NAME_RESTRICTED_CHARS_LIST = ['[', ']', '{', '}', '(', ')' '|']
FIELD_NAME_RESTRICTED_CHARS_EXPRESSION = r'(\[|\]|\{|\}|\(|\)|\|)'


def _raise_if_field_name_is_invalid(field_name: str):
    match: Optional[List[tuple]] = re.findall(pattern=FIELD_NAME_RESTRICTED_CHARS_EXPRESSION, string=field_name)
    if match is not None and len(match) > 0:
        raise InvalidFieldNameException(message_with_vars(
            message="A field name was using one or multiple restricted chars",
            vars_dict={
                'fieldName': field_name, 'restrictedCharsMatch': match,
                'FIELD_NAME_RESTRICTED_CHARS_LIST': FIELD_NAME_RESTRICTED_CHARS_LIST
            }
        ))


class BaseDataModel:
    def __init__(self):
        self.childrens_map: Optional[dict] = None

class MapModel(BaseDataModel):
    _default_primitive_type = dict
    required_fields: list = None

    def __init__(self, **kwargs):
        super().__init__()
        self.kwargs = kwargs
        # from StructNoSQL import field_loader
        # field_loader.load(class_instance=self, **kwargs)

    def new(self):
        pass

    @property
    def dict(self) -> dict:
        return self.childrens_map if self.childrens_map is not None else self.kwargs


class TableDataModel(MapModel):
    # The TableDataModel inherit from MapModel, to allow easier validation of record data.
    # For example, when the put_record function is used, and needs data validation.
    def class_add_field(self, field_key: str, field_item: Any):
        setattr(self.__class__, field_key, field_item)

    def class_from_fields(self, fields: Dict[str, Any]):
        for key, item in fields.items():
            self.class_add_field(field_key=key, field_item=item)

    def instance_add_field(self, field_key: str, field_item: Any):
        self.__dict__[field_key] = field_item

    def instance_from_fields(self, fields: Dict[str, Any]):
        for key, item in fields.items():
            self.instance_add_field(field_key=key, field_item=item)


def _alias_to_model(alias: _GenericAlias):
    alias_variable_name: Optional[str] = alias.__dict__.get('_name', None)
    if alias_variable_name is not None:
        alias_args: Optional[Tuple] = alias.__dict__.get('__args__', None)
        if alias_args is not None:
            if alias_variable_name == "Dict":
                return DictModel(key_type=alias_args[0], item_type=alias_args[1])
            elif alias_variable_name == "Set":
                raise Exception("not yet implemented")
            elif alias_variable_name == "List":
                raise Exception("not yet implemented")
    return None


class BaseItem:
    _table = None
    _database_path: Optional[List[DatabasePathElement]] = None
    _key_expected_type: Optional[type] = None
    _items_excepted_type: Optional[type or MapModel] = None
    # We set the _database_path as static, so that the assign_internal_mapping_from_class can setup the path only once,
    # only by having access to the inheritor class type, not even the instance. Yet, when set the _database_path
    # statically, the value is not attributed to the BaseField class (which would cause to have multiple classes override
    # the paths of the others), but rather, it is statically set on the class that inherit from BaseField.

    def __init__(self, field_type: Optional[type] = Any, custom_default_value: Optional[Any] = None):
        # todo: add a file_url field_type
        self._value = None
        self._query = None

        self.map_key_expected_type: Optional[type] = None
        self.map_model: Optional[MapModel or type] = None

        self._custom_default_value = custom_default_value
        self._field_type = field_type
        self._default_field_type = field_type

        if isinstance(self._field_type, (list, tuple)):
            if len(self._field_type) > 0:
                self._default_field_type = self._field_type[0]

        elif isinstance(self._field_type, _GenericAlias):
            alias_variable_name: Optional[str] = self._field_type.__dict__.get('_name', None)
            if alias_variable_name is not None:
                alias_args: Optional[Tuple] = self._field_type.__dict__.get('__args__', None)

                if alias_variable_name == "Dict":
                    self._field_type = dict
                    self._default_field_type = dict
                    self._key_expected_type = alias_args[0]
                    if self._key_expected_type not in ACCEPTABLE_KEY_TYPES:
                        raise Exception(message_with_vars(
                            message="Key in a Dict field was not found in the acceptable key types",
                            vars_dict={'_key_expected_type': self._key_expected_type, 'ACCEPTABLE_KEY_TYPES': ACCEPTABLE_KEY_TYPES}
                        ))

                    self._items_excepted_type = alias_args[1]
                    if isinstance(self._items_excepted_type, _GenericAlias):
                        self._items_excepted_type = _alias_to_model(alias=self._items_excepted_type)

                elif alias_variable_name == "Set":
                    self._field_type = set
                    self._default_field_type = set
                    self._items_excepted_type = alias_args[0]
                    if isinstance(self._items_excepted_type, _GenericAlias):
                        self._items_excepted_type = _alias_to_model(alias=self._items_excepted_type)
                    # todo: rename the _items_excepted_type variable
                elif alias_variable_name == "List":
                    self._field_type = list
                    self._default_field_type = list
                    self._items_excepted_type = alias_args[0]
                    if isinstance(self._items_excepted_type, _GenericAlias):
                        self._items_excepted_type = _alias_to_model(alias=self._items_excepted_type)
                    # todo: rename the _items_excepted_type variable

        elif MapModel in getattr(self._field_type, '__mro__', ()):
            self.map_model = self._field_type
            self._field_type = dict
            self._default_field_type = dict

        elif self._field_type == dict:
            # Handle an untyped dict
            self._items_excepted_type = Any
            self._key_expected_type = Any
        elif self._field_type == list:
            # Handle an untyped list
            self._items_excepted_type = Any
        elif self._field_type == set:
            # Raise on an untyped set
            raise UsageOfUntypedSetException()

    def validate_data(self) -> Tuple[Optional[Any], bool]:
        from StructNoSQL.validator import validate_data
        validated_data, valid = validate_data(
            value=self._value, item_type_to_return_to=self,
            expected_value_type=self._field_type,
        )
        self._value = validated_data
        return validated_data, valid

    def populate(self, value: Any):
        self._value = value
        # self.validate_data()
        # print("Finished data validation.")

    def query(self, key_value: str, fields_paths_elements: List[str], index_name: Optional[str] = None, query_kwargs: Optional[dict] = None) -> Query:
        """for path_element in self._database_path:
            if "$key$:" in path_element.element_key:
                variable_name = path_element.element_key.replace("$key$:", "")
                if query_kwargs is not None:
                    matching_kwarg = query_kwargs.get(variable_name, None)
                    if matching_kwarg is not None:
                        path_element.element_key = matching_kwarg
                    else:
                        raise Exception(message_with_vars(
                            message="A variable was required but not found in the query_kwargs dict passed to the query function.",
                            vars_dict={"keyVariableName": variable_name, "matchingKwarg": matching_kwarg, "queryKwargs": query_kwargs, "databasePath": self._database_path}
                        ))
                else:
                    raise Exception(message_with_vars(
                        message="A variable was required but not query_kwargs have been passed to the query function.",
                        vars_dict={"keyVariableName": variable_name, "queryKwargs": query_kwargs, "databasePath": self._database_path}
                    ))"""

        self._query = Query(
            variable_validator=self, table=self._table,
            target_database_path=self._database_path,
            index_name=index_name, key_value=key_value,
        )
        return self._query

    def get_default_value(self):
        if self._custom_default_value is not None:
            return self._custom_default_value
        # We initialize the default field type
        # and return it as the default value.
        return self._field_type()

    @property
    def custom_default_value(self) -> Optional[Any]:
        return self._custom_default_value

    @property
    def value(self) -> Any:
        return self._value

    @property
    def field_type(self) -> Any:
        return self._field_type

    @property
    def default_field_type(self) -> type:
        return self._default_field_type

    @property
    def key_expected_type(self) -> Optional[type]:
        return self._key_expected_type

    @property
    def items_excepted_type(self) -> Optional[type or MapModel]:
        return self._items_excepted_type

    @property
    def database_path(self) -> Optional[List[DatabasePathElement]]:
        return self._database_path

    @property
    def table(self):
        return self._table

    @staticmethod
    def instantiate_default_value_type(value_type: type) -> Optional[Any]:
        if value_type == Any:
            return None
        else:
            try:
                return value_type()
            except Exception as e:
                print(e)
                return None


class BaseField(BaseItem):
    def __init__(
            self, name: str, field_type: Optional[Any] = None, required: Optional[bool] = False, not_modifiable: Optional[bool] = False,
            custom_default_value: Optional[Any] = None, key_name: Optional[str] = None, max_nested_depth: Optional[int] = 32
    ):
        super().__init__(field_type=field_type if field_type is not None else Any, custom_default_value=custom_default_value)
        self._name = name
        if self._name is not None:
            _raise_if_field_name_is_invalid(field_name=self._name)
        self._required = required
        self._key_name = None

        if max_nested_depth is not None and max_nested_depth > 32:
            raise Exception(f"DynamoDB support a maximum depth of nested of items of 32. This is not imposed by StructNoSQL but a platform limitation.\n"
                            f"See : https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-attributes")
        self._max_nested = max_nested_depth

        if not_modifiable is True:
            raise Exception(f"Not modifiable not yet implemented")

        if key_name is not None:
            if field_type in [dict, set, list] or type(field_type) is _GenericAlias:
                self._key_name = key_name
            elif isinstance(field_type, (tuple, list)):
                raise Exception(f"Multiple dictionaries are not yet supported.")
                all_items_are_dict = True
                for item in field_type:
                    item_type = type(item)
                    if not isinstance(item, (dict, _GenericAlias)):
                        all_items_are_dict = False
                        break
                if all_items_are_dict is True:
                    self._key_name = key_name
                else:
                    raise Exception(message_with_vars(
                        message="key_name cannot be set on a field that is a tuple or list that does not exclusively contains dict or Dict items",
                        vars_dict={'fieldName': name, 'fieldType': field_type, 'keyName': key_name}
                    ))
            else:
                raise Exception(message_with_vars(
                    message="key_name cannot be set on a field that is not of type dict, Dict, list, List, set or Set",
                    vars_dict={'fieldName': name, 'fieldType': field_type, 'keyName': key_name}
                ))
        else:
            if field_type in [dict, set] or type(field_type) == _GenericAlias:
                self._key_name = f"{name}Key"

    def populate(self, value: any):
        super().populate(value=value)

    @property
    def dict_item(self):
        """ :return: BaseField """
        if self._field_type == dict and self.items_excepted_type is not None:
            map_item = MapItem(parent_field=self, field_type=self.items_excepted_type, model_type=self.items_excepted_type)
            return map_item
        else:
            raise Exception(message_with_vars(
                message="Tried to access dict item of a field that was not of type dict, "
                        "Dict or did not properly received the expected type of the dict items.",
                vars_dict={"fieldName": self.field_name, "fieldType": self.field_type,
                           "dictItemsExceptedType": self.items_excepted_type}
            ))

    def post(self, value: any):
        print(self._database_path)

    @property
    def field_name(self) -> str:
        return self._name

    @property
    def key_name(self) -> Optional[str]:
        return self._key_name

    @property
    def required(self) -> bool:
        return self._required

    @property
    def max_nested(self) -> int:
        # todo: find a better name than max_nested
        return self._max_nested

    def copy(self):
        # return BaseField(name=self._name, field_type=self._field_type, required=self._required, custom_default_value=self._custom_default_value, index_name=self._key_name, max_nested_depth=self._max_nested)
        from copy import copy
        return copy(self)

class MapItem(BaseField):
    _default_primitive_type = dict

    def __init__(self, parent_field: BaseField, model_type: type, field_type: type):
        super().__init__(name=None, field_type=field_type, custom_default_value=BaseItem.instantiate_default_value_type(field_type))
        self.map_model = model_type

        from StructNoSQL.tables.base_table import make_dict_key_var_name, try_to_get_primitive_default_type_of_item
        element_key = make_dict_key_var_name(parent_field.key_name)
        default_type = try_to_get_primitive_default_type_of_item(parent_field.items_excepted_type)
        database_path_element = DatabasePathElement(element_key=element_key, default_type=default_type)
        self._database_path = [*parent_field.database_path, database_path_element]
        self._table = parent_field.table

class DictModel(BaseItem):
    _default_primitive_type = dict

    def __init__(self, key_type: Any, item_type: Any, key_name: Optional[str] = None):
        super().__init__()
        self.key_type = key_type
        self.item_type = item_type
        self.key_name = key_name
        self._database_path = None

    @property
    def database_path(self):
        return self._database_path
