from typing import List, Optional, Any, Dict, _GenericAlias, Tuple
from structnosql.query import Query


class TableDataModel:
    pass


class BaseItem:
    _table = None
    _database_path: Dict[str, type] = None
    # We set the _database_path as static, so that the assign_internal_mapping_from_class can setup the path only once,
    # only by having access to the inheritor class type, not even the instance. Yet, when set the _database_path
    # statically, the value is not attributed to the BaseItem class (which would cause to have multiple classes override
    # the paths of the others), but rather, it is statically set on the class that inherit from BaseItem.

    def __init__(self, name: str, field_type: Optional[type] = Any, custom_default_value: Optional[Any] = None):
        self._value = None
        self._query = None

        self._field_type = field_type
        self._custom_default_value = custom_default_value

        self._name = name
        self.map_model: Optional[MapModel] = None

        """kwargs = {"__root__": (field_type, ...)}
        self._validation_model = create_model(str(self.__class__), **kwargs)
        print(self._validation_model)"""

    def validate_data(self) -> bool:
        from structnosql.validator import validate_data
        return validate_data(value=self._value, expected_value_type=self._field_type, map_model=self.map_model)

    def populate(self, value: Any) -> bool:
        self._value = value
        return self.validate_data()

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
    def value(self) -> Any:
        return self._value


class BaseField(BaseItem):
    def __init__(self, name: str, field_type: Optional[type] = None, custom_default_value: Optional[Any] = None):
        super().__init__(name=name, field_type=field_type if field_type is not None else Any, custom_default_value=custom_default_value)

    def populate(self, value: any):
        super().populate(value=value)


class BaseDataModel:
    def __init__(self):
        self.childrens_map = dict()

class MapModel(BaseDataModel):
    def __init__(self, **kwargs):
        super().__init__()
        from structnosql import field_loader
        field_loader.load(class_instance=self, **kwargs)

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



