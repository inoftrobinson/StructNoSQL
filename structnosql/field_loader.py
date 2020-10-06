from typing import Optional, Tuple, Any, List
from StructNoSQL.fields import BaseField, MapField, BaseDataModel, BaseItem, MapModel
from copy import copy


def update_instance_children(parent_class_instance: BaseDataModel, children_attribute_key: str, children_item: Any):
    parent_class_instance.__setattr__(children_attribute_key, children_item)
    if parent_class_instance.childrens_map is None:
        parent_class_instance.childrens_map = dict()
    parent_class_instance.childrens_map[children_attribute_key] = children_item


def populate_data_workflow(parent_class_instance: BaseDataModel, kwargs: dict, class_variable_key: str, class_variable_item: BaseItem):
    matching_kwarg_value = kwargs.get(class_variable_key, None)
    if matching_kwarg_value is not None:
        kwargs.pop(class_variable_key)
    else:
        matching_kwarg_value = class_variable_item.get_default_value()

    if matching_kwarg_value is not None:
        class_variable_item.populate(value=matching_kwarg_value)

    update_instance_children(
        parent_class_instance=parent_class_instance,
        children_attribute_key=class_variable_key,
        children_item=class_variable_item
    )


def load(class_instance: Optional[Any] = None, class_type: Optional[Any] = None, **kwargs) -> Tuple[BaseDataModel, dict]:
    if class_instance is None:
        if class_type is not None:
            class_instance = class_type()
            # No need to copy the instantiated type here, we will do it later on.
        else:
            raise Exception("Missing class instance and no class type"
                            "has been passed to replaced the instance."
                            f"\n  --class_instance:{class_instance}"
                            f"\n  --class_type:{class_type}")

    class_variables = class_instance.__class__.__dict__
    for class_variable_key, class_variable_item in class_variables.items():
        try:
            class_variable_class_type = class_variable_item.__class__
            if class_variable_class_type == BaseField:
                class_variable_item: BaseField
                instance_variable_item = class_instance.__getattribute__(class_variable_key)
                copied_unique_instance_variable_item = copy(instance_variable_item)
                # We always copy an item before populating its data. Its not the prettiest solution, but currently,
                # since the classes are instantiated when the user create a DataModel, if we were setting the data
                # directly in the instance, we would try to use the same single instance to store all of the data
                # (and so, as soon as we receive multiple objects from the database that will be using the same instance
                # in a model, like in the case of a List or Map), the last data element that we received would always
                # write-over the data of the previous element (since the model has a single instance of each field).

                populate_data_workflow(
                    parent_class_instance=class_instance, kwargs=kwargs,
                    class_variable_key=class_variable_key,
                    class_variable_item=copied_unique_instance_variable_item
                )

            elif class_variable_class_type == MapField:
                class_variable_item: MapField

                instance_variable_item = class_instance.__getattribute__(class_variable_key)
                copied_unique_instance_variable_item: MapField = copy(instance_variable_item)

                map_model_instance = copied_unique_instance_variable_item.map_model()
                _, consumed_nested_kwargs = load(class_instance=map_model_instance, **kwargs.get(class_variable_item.field_name, None) or {})
                # We have no need to store the return instance model (which we pass to _ in order to not create a new variable reference), because the instance
                # is directly modified  during the execution of the function. I still made the function return the instance, just for code readability reasons.

                if consumed_nested_kwargs is None or len(consumed_nested_kwargs) == 0:
                    # The load function will only consume the kwargs little by little, so if we consume all the nested
                    # kwargs of map, we will end up with an empty dict, which will still existed and throw up an error.
                    # So, if the consumed kwargs that we received is either None (which we check before the length,
                    # to avoid potential errors), or if it is empty, we pop it from the parent kwargs.
                    kwargs.pop(class_variable_item.field_name)

                copied_unique_instance_variable_item._value = map_model_instance
                # The populate_data_workflow would not be appropriate for a MapField, so we just set directly the
                # private _value, in order to bypass the validation of the populate function, which has already
                # been done while the items have been populated, and which only work on primitive Python data
                # object (dict, list, str, etc), and not on populated models objects.
                update_instance_children(
                    parent_class_instance=class_instance,
                    children_attribute_key=class_variable_key,
                    children_item=copied_unique_instance_variable_item
                )
                # Then, we can update the reference of the parent object to the instance children.

        except Exception as e:
            print(e)

    if len(kwargs) > 0:
        print(f"WARNING - Some kwargs have been specified, but no corresponding model"
              f" properties have been found. The kwargs have not been used."
              f"\n  --unusedKwargs:{kwargs}"
              f"\n  --classInstance:{class_instance}")

    return class_instance, kwargs
