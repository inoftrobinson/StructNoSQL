from typing import List, Optional, Any, Dict, _GenericAlias, Tuple

from StructNoSQL.dummy_object import DummyObject
from StructNoSQL.dynamodb.models import DatabasePathElement
from StructNoSQL.practical_logger import exceptions_with_vars_message
from StructNoSQL.query import Query


class TableDataModel:
    pass


class BaseItem:
    _table = None
    _database_path: List[DatabasePathElement] = None
    # We set the _database_path as static, so that the assign_internal_mapping_from_class can setup the path only once,
    # only by having access to the inheritor class type, not even the instance. Yet, when set the _database_path
    # statically, the value is not attributed to the BaseItem class (which would cause to have multiple classes override
    # the paths of the others), but rather, it is statically set on the class that inherit from BaseItem.

    def __init__(self, name: str, field_type: Optional[type] = Any, required: Optional[bool] = False, custom_default_value: Optional[Any] = None):
        self._value = None
        self._query = None
        self._name = name
        self._key_name = None

        self.map_key_expected_type: Optional[type] = None
        self.map_model: Optional[MapModel] = None
        self.dict_key_expected_type: Optional[type] = None
        self.dict_value_excepted_type: Optional[type or MapModel] = None

        self._required = required
        self._custom_default_value = custom_default_value
        self._field_type = field_type
        if isinstance(self._field_type, _GenericAlias):
            alias_variable_name: Optional[str] = self._field_type.__dict__.get("_name", None)
            if alias_variable_name is not None:
                alias_args: Optional[Tuple] = self._field_type.__dict__.get("__args__", None)

                if alias_variable_name == "Dict":
                    self._field_type = dict
                    self.dict_key_expected_type = alias_args[0]
                    self.dict_value_excepted_type = alias_args[1]
                    """variable_item._database_path = {**current_path_elements}
                    output_mapping[variable_key] = assign_internal_mapping_from_class(
                        table=table, class_type=variable_item, current_path_elements=variable_item._database_path
                    )"""
                elif alias_variable_name == "List":
                    raise Exception(f"List not yet implemented.")

    def validate_data(self, load_data_into_objects: bool):
        from StructNoSQL.validator import validate_data
        validated_data = validate_data(
            value=self._value, item_type_to_return_to=self, load_data_into_objects=load_data_into_objects,
            expected_value_type=self._field_type, map_model=self.map_model,
            dict_value_excepted_type=self.dict_value_excepted_type, dict_excepted_key_type=self.dict_key_expected_type
        )
        self._value = validated_data

    def populate(self, value: Any):
        self._value = value
        # self.validate_data()
        # print("Finished data validation.")

    def query(self, key_name: str, key_value: str, index_name: Optional[str] = None, query_kwargs: Optional[dict] = None) -> Query:
        for path_element in self._database_path:
            if "$key$:" in path_element.element_key:
                variable_name = path_element.element_key.replace("$key$:", "")
                if query_kwargs is not None:
                    matching_kwarg = query_kwargs.get(variable_name, None)
                    if matching_kwarg is not None:
                        path_element.element_key = matching_kwarg
                    else:
                        raise Exception(exceptions_with_vars_message(
                            message="A variable was required but not found in the query_kwargs dict passed to the query function.",
                            vars_dict={"keyVariableName": variable_name, "matchingKwarg": matching_kwarg, "queryKwargs": query_kwargs, "databasePath": self._database_path}
                        ))
                else:
                    raise Exception(exceptions_with_vars_message(
                        message="A variable was required but not query_kwargs have been passed to the query function.",
                        vars_dict={"keyVariableName": variable_name, "queryKwargs": query_kwargs, "databasePath": self._database_path}
                    ))

        self._query = Query(
            variable_validator=self, table=self._table,
            target_database_path=self._database_path,
            key_name=key_name, key_value=key_value, index_name=index_name
        )
        return self._query

    def post(self, value: any):
        print(self._database_path)

    def get_default_value(self):
        if self._custom_default_value is not None:
            return self._custom_default_value
        # We initialize the default field type
        # and return it as the default value.
        return self._field_type()

    @property
    def field_name(self) -> str:
        return self._name

    @property
    def field_type(self) -> type:
        return self._field_type

    @property
    def key_name(self) -> Optional[str]:
        return self._key_name

    @property
    def required(self) -> bool:
        return self._required

    @property
    def value(self) -> Any:
        return self._value

    @property
    def database_path(self):
        return self._database_path


class BaseField(BaseItem):
    def __init__(self, name: str, field_type: Optional[type] = None, required: Optional[bool] = False,
                 custom_default_value: Optional[Any] = None, key_name: Optional[str] = None):
        super().__init__(name=name, field_type=field_type if field_type is not None else Any,
                         required=required, custom_default_value=custom_default_value)

        if key_name is not None:
            if field_type == dict or type(field_type) == _GenericAlias:
                self._key_name = key_name
            else:
                raise Exception(exceptions_with_vars_message(
                    "key_name cannot be set on a field that is not of type dict or Dict",
                    vars_dict={"fieldName": name, "fieldType": field_type, "keyName": key_name}
                ))
        else:
            if field_type == dict or type(field_type) == _GenericAlias:
                self._key_name = f"{name}Key"

    def populate(self, value: any):
        super().populate(value=value)

    def __getattr__(self, key):
        item = self.__dict__.get(key, None)
        if item is not None:
            return item
        else:
            if self._field_type == dict:
                dict_expected_type_item = self.dict_value_excepted_type.__dict__.get(key, None)
                if dict_expected_type_item is not None:
                    return dict_expected_type_item
        raise AttributeError()


class BaseDataModel:
    def __init__(self):
        self.childrens_map = dict()

class MapModel(BaseDataModel):
    _default_primitive_type = dict

    def __init__(self, **kwargs):
        super().__init__()
        # from StructNoSQL import field_loader
        # field_loader.load(class_instance=self, **kwargs)

    def new(self):
        pass

    @property
    def dict(self) -> dict:
        return self.childrens_map


class MapField(BaseItem):
    def __init__(self, name: str, model):
        super().__init__(name=name, field_type=dict)
        self.map_model: type(MapModel) = model
        # self.populate(value=model().dict)
        # todo: create models to validate the dicts


class ListField(BaseItem):
    def __init__(self, name: str, items_model: Optional[MapModel] = None):
        super().__init__(name=name, field_type=list)
        self._list_items_model = items_model
        # todo: allow to have multiple items_model and that an item can be one of many item models
        # self.populate(value=model().dict)



