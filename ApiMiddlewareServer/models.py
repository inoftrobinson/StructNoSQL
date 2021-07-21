from typing import Optional, Any
from pydantic import BaseModel
from StructNoSQL import NoneType


class SerializedType(str):
    _keys_to_types = {'str': str, 'int': int, 'float': float, 'dict': dict, 'list': list, 'NoneType': NoneType}

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value) -> type:
        if not isinstance(value, str):
            raise TypeError('string required')
        matching_type: Optional[type] = SerializedType._keys_to_types.get(value, None)
        if matching_type is None:
            raise TypeError('invalid type')
        return matching_type

class FieldPathElementItemModel(BaseModel):
    elementKey: str
    defaultType: SerializedType or type
    customDefaultValue: Optional[Any] = None
