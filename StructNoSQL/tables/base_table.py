from typing import Optional, List, Any, Set, _GenericAlias
from copy import copy

from StructNoSQL.dynamodb.dynamodb_core import DynamoDbCoreAdapter, PrimaryIndex, GlobalSecondaryIndex
from StructNoSQL.dynamodb.models import DatabasePathElement
from StructNoSQL.fields import BaseField, MapItem, TableDataModel, DictModel, MapModel
from StructNoSQL.practical_logger import message_with_vars
from StructNoSQL.utils.types import PRIMITIVE_TYPES, TYPED_TYPES_TO_PRIMITIVES


# todo: add ability to add or remove items from list's

class DatabaseKey(str):
    pass


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
        self, table_name: str, region_name: str,
        data_model, primary_index: PrimaryIndex,
        billing_mode: str = DynamoDbCoreAdapter.PAY_PER_REQUEST,
        global_secondary_indexes: List[GlobalSecondaryIndex] = None,
        auto_create_table: bool = True
    ):
        self.fields_switch = FieldsSwitch()
        self._internal_mapping = dict()
        self._dynamodb_client = DynamoDbCoreAdapter(
            table_name=table_name, region_name=region_name, billing_mode=billing_mode,
            primary_index=primary_index, global_secondary_indexes=global_secondary_indexes,
            create_table=auto_create_table
        )
        self._primary_index_name = primary_index.index_custom_name or primary_index.hash_key_name

        if not isinstance(data_model, type):
            self._model = data_model
        else:
            self._model = data_model()
        self._model_virtual_map_field = None

        self.processed_class_types: Set[type] = set()
        Processor(table=self).assign_internal_mapping_from_class(class_instance=self._model)

    @property
    def primary_index_name(self) -> str:
        return self._primary_index_name

    @property
    def model(self) -> TableDataModel:
        return self._model

    @property
    def model_virtual_map_field(self) -> BaseField:
        if self._model_virtual_map_field is None:
            self._model_virtual_map_field = BaseField(name="", field_type=self._model.__class__)
            # The model_virtual_map_field is a BaseField with no name, that use the table model class type, which easily
            # give us the ability to use the functions of the BaseField object (for example, functions for data validation),
            # with the data model of the table itself, without having to create an intermediary item. For example, the
            # put_record operation, needs to validate its data, based on the table data model, not a BaseField.
        return self._model_virtual_map_field

    @property
    def internal_mapping(self) -> dict:
        return self._internal_mapping

    @property
    def dynamodb_client(self) -> DynamoDbCoreAdapter:
        return self._dynamodb_client


def make_dict_key_var_name(key_name: str) -> str:
    return f"$key$:{key_name}"

def try_to_get_primitive_default_type_of_item(item_type: Any):
    item_default_primitive_type: Optional[type] = getattr(item_type, '_default_primitive_type', None)
    if item_default_primitive_type is not None:
        return item_default_primitive_type

    item_type_name: Optional[str] = getattr(item_type, '_name', None)
    if item_type_name is not None:
        primitive_from_typed: Optional[type] = TYPED_TYPES_TO_PRIMITIVES.get(item_type_name, None)
        if primitive_from_typed is not None:
            return primitive_from_typed

    return item_type


class Processor:
    def __init__(self, table: BaseTable):
        self.table = table

    def process_item(
            self, class_type: Optional[type], variable_item: Any, current_field_path: str,
            current_path_elements: Optional[List[DatabasePathElement]] = None, is_nested: bool = False
    ) -> list:
        required_fields = list()
        if current_path_elements is None:
            current_path_elements = list()

        try:
            if isinstance(variable_item, DictModel):
                variable_item: DictModel

                new_database_path_element = DatabasePathElement(
                    element_key=variable_item.key_name,
                    default_type=variable_item.item_type
                )
                variable_item._database_path = [*current_path_elements, new_database_path_element]
                variable_item._table = self.table

                """if variable_item.required is True:
                    required_fields.append(variable_item)"""

                current_field_path += ("" if len(current_field_path) == 0 else ".") + "{{" + variable_item.key_name + "}}"
                field_is_valid = self.table.fields_switch.set(key=current_field_path, item=copy(variable_item))

            elif isinstance(variable_item, BaseField):
                variable_item: BaseField
                new_database_path_element = DatabasePathElement(
                    element_key=variable_item.field_name,
                    default_type=variable_item.default_field_type,
                    custom_default_value=variable_item.custom_default_value
                )
                variable_item._database_path = [*current_path_elements, new_database_path_element]
                variable_item._table = self.table

                if variable_item.required is True:
                    required_fields.append(variable_item)

                current_field_path += f"{variable_item.field_name}" if len(current_field_path) == 0 else f".{variable_item.field_name}"
                field_is_valid = self.table.fields_switch.set(key=current_field_path, item=copy(variable_item))
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
                                        nested_variable_item = variable_item.copy()
                                        nested_variable_item._database_path = [*current_nested_database_path]
                                        item_rendered_key_name = nested_variable_item.key_name.replace("{i}", f"{i}")

                                        map_item = MapItem(
                                            parent_field=nested_variable_item,
                                            field_type=nested_variable_item.default_field_type,
                                            model_type=nested_variable_item.items_excepted_type
                                        )
                                        if i > 0:
                                            current_nested_field_path += f".{variable_item.field_name}"
                                        current_nested_field_path += ".{{" + item_rendered_key_name + "}}"

                                        current_nested_database_path.append(DatabasePathElement(
                                            element_key=make_dict_key_var_name(item_rendered_key_name),
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
                            current_field_path += ".{{" + variable_item.key_name + "}}"

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
                                            class_type=None,
                                            variable_item=variable_item.items_excepted_type,
                                            current_field_path=current_field_path,
                                            current_path_elements=current_path_elements
                                        )

                                    self.assign_internal_mapping_from_class(
                                        class_type=variable_item.items_excepted_type,
                                        nested_field_path=current_field_path,
                                        current_path_elements=current_path_elements
                                    )

        except Exception as e:
            print(e)

        return required_fields

    def assign_internal_mapping_from_class(
        self, class_instance: Optional[Any] = None, class_type: Optional[Any] = None,
        current_path_elements: Optional[List[DatabasePathElement]] = None,
        nested_field_path: Optional[str] = None, is_nested: Optional[bool] = False
    ):
        if class_type is None:
            if class_instance is not None:
                class_type = class_instance.__class__
            else:
                raise Exception(message_with_vars(
                    message="class_type or class_instance args must be passed "
                            "to the assign_internal_mapping_from_class function"
                ))

        # todo: re-implement some king of processed class types to avoid initializing
        #  multiple times the same class when we have a nested class ?
        if class_type in self.table.processed_class_types:
            pass
            # return None
        else:
            pass

        class_variables = class_type.__dict__

        setup_function: Optional[callable] = class_variables.get('__setup__', None)
        if setup_function is not None:
            custom_setup_class_variables: dict = class_type.__setup__()
            if len(custom_setup_class_variables) > 0:
                # The class_variables gotten from calling the __dict__ attribute is a mappingproxy, which cannot be modify.
                # In order to combine the custom_setup_class_variables and the class_variables variables we will iterate
                # over all the class_variables attributes, add them to the dict create by the __setup__ function (only if
                # they are not found in the custom_setup_class_variables dict, since the custom setup override any default
                # class attribute), and assign the class_variables variable to our newly create and setup dict.
                for key, item in class_variables.items():
                    if key not in custom_setup_class_variables:
                        custom_setup_class_variables[key] = item
                class_variables = custom_setup_class_variables

        required_fields: List[BaseField] = list()
        for variable_item in class_variables.values():
            current_field_path = "" if nested_field_path is None else nested_field_path
            required_fields.extend(self.process_item(
                class_type=class_type,
                variable_item=variable_item,
                current_field_path=current_field_path,
                current_path_elements=current_path_elements,
                is_nested=is_nested
            ))

        setattr(class_type, "required_fields", required_fields)
        # We need to set the attribute, because when we go the required_fields with the get_attr
        # function, we did not get a reference to the attribute, but a copy of the attribute value.
