from typing import Optional

from StructNoSQL.utils.types import TYPED_TYPES_TO_PRIMITIVES


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
