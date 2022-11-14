import logging
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from typing import Optional, List, Any, Set, _GenericAlias, Callable, Dict, Type

from StructNoSQL.models import DatabasePathElement, FieldRemover
from StructNoSQL.fields import BaseField, MapItem, TableDataModel, DictModel, MapModel, BaseItem
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.tables_clients.backend import PrimaryIndex
from StructNoSQL.utils.misc_fields_items import try_to_get_primitive_default_type_of_item, make_dict_key_var_name
from StructNoSQL.utils.types import PRIMITIVE_TYPES


# todo: add ability to add or remove items from list's


class FieldsSwitch(dict):
    def __init__(self, *args, **kwargs):
        super(dict).__init__(*args, **kwargs)

    def set(self, key: str, item: BaseField) -> bool:
        if len(item.database_path) > 32:
            print("\nDynamoDB support a maximum depth of nested of items of 32. This is not imposed by StructNoSQL but by DynamoDB.\n"
                  "See : https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-attributes")
            return False
        else:
            self.__setitem__(key, item)
            return True


class BaseTable:
    def __init__(
            self, data_model: Type[TableDataModel], primary_index: PrimaryIndex,
            auto_leading_key: Optional[str] = None
    ):
        self.fields_switch = FieldsSwitch()
        self._internal_mapping = {}

        model_copy = data_model.copy()

        self._model = model_copy
        if TableDataModel not in self._model.__bases__:
            raise Exception("TableModel must inherit from TableDataModel class")
        self._model_virtual_map_field = None

        self._primary_index_name = primary_index.index_custom_name or primary_index.hash_key_name

        self.processed_class_types: Set[type] = set()
        Processor(table=self).assign_internal_mapping_from_class(class_type=self._model)

        if auto_leading_key is not None:
            def remove_auto_leading_key(value: Any):
                import re
                matches: Optional[re.Match] = re.match(f'({auto_leading_key})(.*)', value)
                if matches is not None:
                    return matches.group(2)
                else:
                    logging.warning("auto_leading_key not found in returned data, None is being returned")
                    return None

            primary_key_field_object: BaseItem = self._get_primary_key_field()
            primary_key_field_object.write_transformers.insert(0, lambda value: f"{auto_leading_key}{value}")
            primary_key_field_object.read_transformers.insert(0, remove_auto_leading_key)

    @property
    def model(self) -> TableDataModel:
        return self._model

    @property
    def model_virtual_map_field(self) -> BaseField:
        if self._model_virtual_map_field is None:
            self._model_virtual_map_field = BaseField(field_type=self.model, required=True)
            # The model_virtual_map_field is a BaseField with no name, that use the table model class type, which easily
            # give us the ability to use the functions of the BaseField object (for example, functions for data validation),
            # with the data model of the table itself, without having to create an intermediary item. For example, the
            # put_record operation, needs to validate its data, based on the table data model, not a BaseField.
        return self._model_virtual_map_field

    @property
    def internal_mapping(self) -> dict:
        return self._internal_mapping

    @property
    def primary_index_name(self) -> str:
        return self._primary_index_name

    @staticmethod
    def _async_field_removers_executor(task_executor: Callable[[FieldRemover], Any], removers: Dict[str, FieldRemover]) -> Dict[str, Any]:
        # This function is used both to run delete_field and remove_field operations asynchronously
        if not len(removers) > 0:
            return {}
        with ThreadPoolExecutor(max_workers=len(removers)) as executor:
            return {key: executor.submit(task_executor, item).result() for key, item in removers.items()}

    def _get_primary_key_field(self) -> BaseItem:
        primary_key_field_object: Optional[BaseItem] = self.fields_switch.get(self.primary_index_name, None)
        # todo: replace primary_index_name by primary_key_name
        if primary_key_field_object is None:
            raise Exception("Could not find a field object for primary_index_name")
        return primary_key_field_object

    def _get_primary_key_database_path(self) -> List[DatabasePathElement]:
        primary_key_field_object: BaseItem = self._get_primary_key_field()
        return primary_key_field_object.database_path


class Processor:
    def __init__(self, table: BaseTable):
        self.table = table

    def process_item(
            self, item_key_name: Optional[str], class_type: Optional[type], variable_item: Any, current_field_path: str,
            current_path_elements: Optional[List[DatabasePathElement]] = None, is_nested: bool = False
    ) -> list:
        required_fields = []
        if current_path_elements is None:
            current_path_elements = []

        field_is_valid: bool = False
        if isinstance(variable_item, DictModel):
            variable_item: DictModel

            """new_database_path_element = DatabasePathElement(
                element_key=variable_item.key_name,
                default_type=variable_item.item_type
            )"""
            variable_item._database_path = [*current_path_elements]  #, new_database_path_element]

            """if variable_item.required is True:
                required_fields.append(variable_item)"""

            current_field_path += ("" if len(current_field_path) == 0 else ".") + "{{" + variable_item.key_name + "}}"
            field_is_valid = self.table.fields_switch.set(key=current_field_path, item=variable_item)

        elif MapModel in getattr(variable_item, '__mro__', ()):
            if len(current_path_elements) > 0:
                self.assign_internal_mapping_from_class(
                    class_type=variable_item,
                    nested_field_path=current_field_path,
                    current_path_elements=[*current_path_elements]
                )

        elif isinstance(variable_item, BaseField):
            variable_item: BaseField
            if item_key_name is not None and variable_item.field_name is None:
                variable_item.field_name = item_key_name

            new_database_path_element = DatabasePathElement(
                element_key=variable_item.field_name,
                default_type=variable_item.default_field_type,
                custom_default_value=variable_item.custom_default_value
            )
            variable_item._database_path = [*current_path_elements, new_database_path_element]

            if variable_item.required is True:
                required_fields.append(variable_item)

            current_field_path += f"{variable_item.field_name}" if len(current_field_path) == 0 else f".{variable_item.field_name}"
            field_is_valid = self.table.fields_switch.set(key=current_field_path, item=variable_item)
            if variable_item.key_name is not None:
                if "{i}" not in variable_item.key_name:
                    # The current_field_path concat is being handled lower in the code for the nested fields
                    current_field_path += ".{{" + variable_item.key_name + "}}"

        if field_is_valid is True:
            if variable_item.map_model is not None:
                self.assign_internal_mapping_from_class(
                    class_type=variable_item.map_model,
                    nested_field_path=current_field_path,
                    current_path_elements=[*variable_item.database_path]
                )

            if variable_item.items_excepted_type is not None:
                from StructNoSQL import ActiveSelf
                if variable_item.items_excepted_type is ActiveSelf:
                    if class_type is None:
                        raise Exception(message_with_vars(
                            message="Cannot use the ActiveSelf attribute as the items_excepted_type when the class type is None",
                            vars_dict={'field_name': variable_item.field_name}
                        ))
                    variable_item._items_excepted_type = class_type

                item_default_type = try_to_get_primitive_default_type_of_item(item_type=variable_item.items_excepted_type)
                item_key_name = make_dict_key_var_name(variable_item.key_name)

                if "{i}" in variable_item.key_name:
                    if is_nested is not True:
                        current_nested_field_path = "" if current_field_path is None else current_field_path
                        current_nested_database_path = [*variable_item.database_path]
                        for i in range(variable_item.max_nested):
                            if len(current_nested_database_path) > 32:
                                print(message_with_vars(
                                    message="Imposed a max nested database depth on field missing or with a too high nested depth limit.",
                                    vars_dict={
                                        'current_field_path': current_field_path,
                                        'field_name': variable_item.field_name,
                                        'imposedMaxNestedDepth': i
                                    }
                                ))
                                break
                            else:
                                nested_variable_item = variable_item
                                item_rendered_key_name: str = nested_variable_item.key_name.replace("{i}", f"{i}")
                                nested_variable_item._database_path = [*current_nested_database_path]
                                nested_variable_item._key_name = item_rendered_key_name
                                # We create a copy of the variable_item unpon which we render the key_name and add
                                # the appropriate database_path_elements into to prepare the creation of the MapItem.

                                map_item = MapItem(
                                    parent_field=nested_variable_item,
                                    field_type=nested_variable_item.default_field_type,
                                    model_type=nested_variable_item.items_excepted_type
                                )
                                # The MapItem will retrieve the key_name of its parent_field when initialized.
                                # Hence, it is important to do the modifications on the nested_variable_item
                                # before the initialization of the MapItem.

                                if i > 0:
                                    current_nested_field_path += f".{variable_item.field_name}"
                                current_nested_field_path += ".{{" + map_item.key_name + "}}"

                                current_nested_database_path.append(DatabasePathElement(
                                    element_key=make_dict_key_var_name(map_item.key_name),
                                    default_type=nested_variable_item.default_field_type,
                                    custom_default_value=nested_variable_item.custom_default_value
                                ))
                                field_is_valid = self.table.fields_switch.set(key=current_nested_field_path, item=map_item)
                                if field_is_valid is True:
                                    if variable_item.items_excepted_type not in PRIMITIVE_TYPES:
                                        self.assign_internal_mapping_from_class(
                                            class_type=variable_item.items_excepted_type,
                                            nested_field_path=current_nested_field_path,
                                            current_path_elements=[*current_nested_database_path],
                                            is_nested=True
                                        )
                                current_nested_database_path.append(DatabasePathElement(
                                    element_key=nested_variable_item.field_name,
                                    default_type=nested_variable_item.default_field_type,
                                    custom_default_value=nested_variable_item.custom_default_value
                                ))
                else:
                    map_item = MapItem(
                        parent_field=variable_item, field_type=item_default_type,
                        model_type=variable_item.items_excepted_type
                    )
                    field_is_valid = self.table.fields_switch.set(current_field_path, map_item)

                    if field_is_valid is True:
                        items_excepted_type = variable_item.items_excepted_type
                        if items_excepted_type not in PRIMITIVE_TYPES:
                            new_database_dict_item_path_element = DatabasePathElement(element_key=item_key_name, default_type=item_default_type)
                            current_path_elements = [*variable_item.database_path, new_database_dict_item_path_element]

                            if isinstance(items_excepted_type, DictModel):
                                if items_excepted_type.key_name is None:
                                    # If the key_name of a DictModel is not defined (for example, when a nested typed Dict is converted
                                    # to a DictModel) we set its key to the key of its parent plus the child keyword. So, a parent key
                                    # of itemKey will give itemKeyChild, and a parent of itemKeyChild will give itemKeyChildChild.
                                    items_excepted_type.key_name = f"{variable_item.key_name}Child"

                            self.process_item(
                                class_type=None, item_key_name=None,
                                variable_item=variable_item.items_excepted_type,
                                current_field_path=current_field_path,
                                current_path_elements=current_path_elements
                            )
                            self.assign_internal_mapping_from_class(
                                class_type=variable_item.items_excepted_type,
                                nested_field_path=current_field_path,
                                current_path_elements=current_path_elements
                            )

        return required_fields

    def assign_internal_mapping_from_class(
        self, class_type: type,
        current_path_elements: Optional[List[DatabasePathElement]] = None,
        nested_field_path: Optional[str] = None, is_nested: Optional[bool] = False
    ):

        # todo: re-implement some king of processed class types to avoid initializing
        #  multiple times the same class when we have a nested class ?
        if class_type in self.table.processed_class_types:
            pass
            # return None
        else:
            pass

        deep_class_variables: dict = {}
        component_classes: Optional[List[type]] = getattr(class_type, '__mro__', None)
        # Instead of just retrieving the __dict__ of the current class_type, we retrieve the __dict__'s of all the
        # classes  in the __mro__ of the class_type (hence, the class type itself, and all of the types it inherited).
        # If we did not do that, fields inherited from a parent class would not be detected and not be indexed.
        if component_classes is not None:
            for component_class in component_classes:
                deep_class_variables.update(component_class.__dict__)

        setup_function: Optional[callable] = deep_class_variables.get('__setup__', None)
        if setup_function is not None:
            custom_setup_deep_class_variables: dict = class_type.__setup__()
            if len(custom_setup_deep_class_variables) > 0:
                # The deep_class_variables gotten from calling the __dict__ attribute is a mappingproxy, which cannot be modify.
                # In order to combine the custom_setup_deep_class_variables and the deep_class_variables variables we will iterate
                # over all the deep_class_variables attributes, add them to the dict create by the __setup__ function (only if
                # they are not found in the custom_setup_deep_class_variables dict, since the custom setup override any default
                # class attribute), and assign the deep_class_variables variable to our newly create and setup dict.
                for key, item in deep_class_variables.items():
                    if key not in custom_setup_deep_class_variables:
                        custom_setup_deep_class_variables[key] = item
                deep_class_variables = custom_setup_deep_class_variables

        required_fields: List[BaseField] = []
        for variable_key_name, variable_item in deep_class_variables.items():
            if isinstance(variable_item, BaseField):
                current_field_path = "" if nested_field_path is None else nested_field_path
                required_fields.extend(self.process_item(
                    class_type=class_type,
                    item_key_name=variable_key_name,
                    variable_item=variable_item,
                    current_field_path=current_field_path,
                    current_path_elements=current_path_elements,
                    is_nested=is_nested
                ))

        setattr(class_type, 'required_fields', required_fields)
        # We need to set the attribute, because when we go the required_fields with the get_attr
        # function, we did not get a reference to the attribute, but a copy of the attribute value.
