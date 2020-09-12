from typing import List, Optional, Any, Dict
from pydantic import create_model

from query import Query



class BaseItem:
    _table = None
    _database_path: Dict[str, type] = None
    # We set the _database_path as static, so that the assign_internal_mapping_from_class can setup the path only once,
    # only by having access to the inheritor class type, not even the instance. Yet, when set the _database_path
    # statically, the value is not attributed to the BaseItem class (which would cause to have multiple classes override
    # the paths of the others), but rather, it is statically set on the class that inherit from BaseItem.

    def __init__(self, name: str, field_type: Optional[type] = Any, custom_default_value: Optional[Any] = None):
        self._name = name
        self.field_type = field_type
        self._map_model: Optional[MapModel] = None
        self._custom_default_value = custom_default_value

        """kwargs = {"__root__": (field_type, ...)}
        self._validation_model = create_model(str(self.__class__), **kwargs)
        print(self._validation_model)"""

    def validate_data(self) -> bool:
        # todo: add recursion to validator
        return validate_data(value=self._value, expected_value_type=self.field_type, map_model=self._map_model)

    def populate(self, value: Any):
        self._value = value
        self.validate_data()

    def query(self, key_name: str, key_value: str, index_name: Optional[str] = None) -> Query:
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
        return self.field_type()

    @property
    def name(self) -> str:
        return self._name


class BaseField(BaseItem):
    def __init__(self, name: str, field_type: Optional[type] = None, default_value: Optional[Any] = None):
        super().__init__(name=name, field_type=field_type if field_type is not None else Any)
        self.field_type = field_type
        self.default_value = default_value

    def populate(self, value: any):
        # todo: implement type checking (use the Pydantic librairy ?)
        super().populate(value=value)


class MapModel:
    def __init__(self, **kwargs):
        self._childrens_map = dict()
        class_variables = self.__class__.__dict__

        for variable_key, variable_item in class_variables.items():
            try:
                variable_class_type = variable_item.__class__
                if variable_class_type == BaseField:
                    variable_item: BaseField

                    matching_kwarg_value = kwargs.get(variable_key, None)
                    if matching_kwarg_value is not None:
                        kwargs.pop(variable_key)
                    else:
                        matching_kwarg_value = variable_item.get_default_value()

                    if matching_kwarg_value is not None:
                        variable_item.populate(value=matching_kwarg_value)

                    self._childrens_map[variable_key] = variable_item
            except Exception as e:
                print(e)

        if len(kwargs) > 0:
            print(f"WARNING - Some kwargs have been specified, but no corresponding model"
                  f"properties have been found. The kwargs have not been used."
                  f"\n  --unusedKwargs:{kwargs}"
                  f"\n  --modelInstance:{self}")

    @property
    def dict(self) -> dict:
        return self._childrens_map




class MapField(BaseItem):
    def __init__(self, name: str, model):
        model: type(MapModel)

        super().__init__(name=name, field_type=dict)
        self._map_model = model
        # self.populate(value=model().dict)
        # todo: create models to validate the dicts


class ListField(BaseItem):
    def __init__(self, name: str, items_model: Optional[MapModel] = None):
        super().__init__(name=name, field_type=list)
        self._list_items_model = items_model
        # todo: allow to have multiple items_model and that an item can be one of many item models
        # self.populate(value=model().dict)


def validate_data(value, expected_value_type: type, map_model: Optional[MapModel] = None,
                  list_items_models: Optional[MapModel] = None) -> bool:
    # todo: add recursion to validator
    value_type = type(value)

    if value_type != expected_value_type:
        raise Exception(f"Data validation exception. The types of an item did not match"
                        f"\n  type(value):{type(value)}"
                        f"\n  expected_value_type:{expected_value_type}")

    if value_type == dict:
        value: dict
        if map_model is not None:
            for key, item in value.items():
                matching_validation_model_variable: Optional[BaseItem] = map_model.__dict__.get(key, None)
                if matching_validation_model_variable is not None:
                    validate_data(value=item, expected_value_type=matching_validation_model_variable.field_type)
                else:
                    raise Exception(f"No map validator was found.")

    elif value_type == list:
        value: list
        if list_items_models is not None:
            for item in value:
                matching_validation_model_variable: Optional[BaseItem] = map_model.__dict__.get(key, None)
                if matching_validation_model_variable is not None:
                    validate_data(value=item, expected_value_type=matching_validation_model_variable.field_type)
                else:
                    raise Exception(f"No map validator was found.")

    return True
