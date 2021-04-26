from typing import Any
from StructNoSQL.fields import BaseDataModel, BaseField


def update_instance_children(parent_class_instance: BaseDataModel, children_attribute_key: str, children_item: Any):
    parent_class_instance.__setattr__(children_attribute_key, children_item)
    if parent_class_instance.childrens_map is None:
        parent_class_instance.childrens_map = dict()
    parent_class_instance.childrens_map[children_attribute_key] = children_item


def populate_data_workflow(parent_class_instance: BaseDataModel, kwargs: dict, class_variable_key: str, class_variable_item: BaseField):
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
