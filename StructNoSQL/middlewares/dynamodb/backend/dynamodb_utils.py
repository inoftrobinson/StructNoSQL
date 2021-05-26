import decimal
from decimal import Decimal, Context
from typing import Any, List, Optional, Callable, Set

from boto3.dynamodb.types import Binary

from StructNoSQL.practical_logger import message_with_vars


def _decimal_to_python(decimal_number: Decimal) -> float or int:
    decimal_float = decimal_number.__float__()
    return decimal_float if decimal_float.is_integer() is not True else int(decimal_float)


def _python_to_decimal(python_number: int or float) -> Decimal:
    return DynamoDBUtils.DECIMAL_DYNAMODB_CONTEXT.create_decimal(python_number)


def _dynamodb_number_to_python(number_string: str):
    float_number = float(number_string)
    if float_number.is_integer():
        return int(float_number)
    else:
        return float_number


def _dynamodb_binary_to_python(binary_data):
    from boto3.dynamodb.types import Binary
    return Binary(value=binary_data)


class DynamoDBUtils:
    TYPE_STRING = 'S'
    TYPE_NUMBER = 'N'
    TYPE_BINARY = 'B'
    TYPE_STRING_SET = 'SS'
    TYPE_NUMBER_SET = 'NS'
    TYPE_BINARY_SET = 'BS'
    TYPE_NULL = 'NULL'
    TYPE_BOOLEAN = 'BOOL'
    TYPE_MAP = 'M'
    TYPE_LIST = 'L'
    DYNAMODB_TYPES_KEYS: List[str] = [
        TYPE_STRING, TYPE_NUMBER, TYPE_BINARY, TYPE_STRING_SET, TYPE_NUMBER_SET,
        TYPE_BINARY_SET, TYPE_NULL, TYPE_BOOLEAN, TYPE_MAP, TYPE_LIST
    ]
    ALL_TYPES_WHERE_VALUE_DO_NOT_NEED_MODIFICATIONS = [TYPE_STRING, TYPE_BOOLEAN]

    DECIMAL_DYNAMODB_CONTEXT = Context(
        prec=38, rounding=decimal.ROUND_HALF_EVEN,
        Emin=-128, Emax=126,
        capitals=1, clamp=0,
        flags=[], traps=[]
    )
    # 38 is the maximum numbers of Decimals numbers that DynamoDB can support.

    from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
    # The serializer and deserializer can be static, even in a lambda,
    # since their classes will never change according to the user.
    _serializer = None
    _deserializer = None

    @classmethod
    def serializer(cls) -> TypeSerializer:
        if cls._serializer is None:
            cls._serializer = cls.TypeSerializer()
        return cls._serializer

    @classmethod
    def deserializer(cls) -> TypeDeserializer:
        if cls._deserializer is None:
            cls._deserializer = cls.TypeDeserializer()
        return cls._deserializer

    @staticmethod
    def python_to_dynamodb(python_object: Any):
        return PythonToDynamoDBValuesConvertor.convert(python_object=python_object)

    @staticmethod
    def dynamodb_to_python(dynamodb_object: Any):
        if isinstance(dynamodb_object, Decimal):
            return _decimal_to_python(decimal_number=dynamodb_object)
        elif isinstance(dynamodb_object, list):
            for i, item in enumerate(dynamodb_object):
                dynamodb_object[i] = DynamoDBUtils.dynamodb_to_python(dynamodb_object=item)
            return dynamodb_object
        elif isinstance(dynamodb_object, dict):
            if len(dynamodb_object) == 1:
                # If the length of the Dict is only one, it might be a DynamoDB object, with its key
                # as its variable type. For example : {'N': '1'}  And yes, this also apply to lists and maps.
                first_key: str = list(dynamodb_object.keys())[0]
                if first_key in DynamoDBUtils.DYNAMODB_TYPES_KEYS:
                    first_item: Any = dynamodb_object[first_key]
                    return DynamoDBToPythonValuesConvertor.convert(first_key=first_key, dynamodb_item=first_item)

            # If the dict was a classic dict, with its first key not in the keys used by DynamoDB
            keys_to_pop: List[str] = []
            for key, item in dynamodb_object.items():
                item_value = DynamoDBUtils.dynamodb_to_python(dynamodb_object=item)
                if item_value is not None:
                    dynamodb_object[key] = item_value
                else:
                    keys_to_pop.append(key)

            for key in keys_to_pop:
                dynamodb_object.pop(key)

            return dynamodb_object
        else:
            return dynamodb_object

    @staticmethod
    def dynamodb_to_python_higher_level(dynamodb_object: Any):
        if isinstance(dynamodb_object, Decimal):
            return _decimal_to_python(decimal_number=dynamodb_object)
        elif isinstance(dynamodb_object, list):
            for i, item in enumerate(dynamodb_object):
                dynamodb_object[i] = DynamoDBUtils.dynamodb_to_python_higher_level(dynamodb_object=item)
            return dynamodb_object
        elif isinstance(dynamodb_object, dict):
            # If the dict was a classic dict, with its first key not in the keys used by DynamoDB
            for key, item in dynamodb_object.items():
                dynamodb_object[key] = DynamoDBUtils.dynamodb_to_python_higher_level(dynamodb_object=item)
            return dynamodb_object
        return dynamodb_object


class DynamoDBToPythonValuesConvertor:
    """This class deserializes DynamoDB types to Python types."""
    """
    DynamoDB                                Python
    --------                                ------
    {'NULL': True}                          None
    {'BOOL': True/False}                    True/False
    {'N': str(value)}                       Decimal(str(value))
    {'S': string}                           string
    {'B': bytes}                            Binary(bytes)
    {'NS': [str(value)]}                    set([Decimal(str(value))])
    {'SS': [string]}                        set([string])
    {'BS': [bytes]}                         set([bytes])
    {'L': list}                             list
    {'M': dict}                             dict
    """

    @staticmethod
    def _handler_n(value: str):
        return _dynamodb_number_to_python(number_string=value)

    @staticmethod
    def _handler_s(value: str):
        return value

    @staticmethod
    def _handler_bool(value: bool):
        return value

    @staticmethod
    def _handler_m(value: dict):
        return dict([(key, DynamoDBUtils.dynamodb_to_python(element)) for key, element in value.items()])

    @staticmethod
    def _handler_l(value: list):
        return [DynamoDBUtils.dynamodb_to_python(element) for element in value]

    @staticmethod
    def _handler_b(value: Any):
        return _dynamodb_binary_to_python(binary_data=value)

    @staticmethod
    def _handler_ns(value: Any) -> Set[int or float]:
        return set(map(DynamoDBUtils.DECIMAL_DYNAMODB_CONTEXT.create_decimal, value))

    @staticmethod
    def _handler_ss(value: Any) -> Set[str]:
        return set(value)

    @staticmethod
    def _handler_bs(value: Any) -> Set[Binary]:
        return set(map(_dynamodb_binary_to_python, value))

    @staticmethod
    def _handler_null(value: Any) -> None:
        return None

    @staticmethod
    def convert(first_key: str, dynamodb_item: Any):
        handler: Optional[Callable[[Any], dict]] = getattr(DynamoDBToPythonValuesConvertor, f'_${first_key.lower()}_handler', None)
        if handler is None:
            raise Exception(f"Type {first_key} not supported")
        return handler(dynamodb_item)


class PythonToDynamoDBValuesConvertor:
    """This class serializes Python types to DynamoDB types."""
    """
    Python                                  DynamoDB
    ------                                  --------
    None                                    {'NULL': True}
    True/False                              {'BOOL': True/False}
    int/Decimal                             {'N': str(value)}
    string                                  {'S': string}
    Binary/bytearray/bytes (py3 only)       {'B': bytes}
    set([int/Decimal])                      {'NS': [str(value)]}
    set([string])                           {'SS': [string])
    set([Binary/bytearray/bytes])           {'BS': [bytes]}
    list                                    {'L': list}
    dict                                    {'M': dict}
    """

    @staticmethod
    def _number_handler(python_object: int or float):
        return {DynamoDBUtils.TYPE_NUMBER: _python_to_decimal(python_number=python_object)}

    @staticmethod
    def _int_handler(python_object: int):
        return PythonToDynamoDBValuesConvertor._number_handler(python_object=python_object)

    @staticmethod
    def _float_handler(python_object: float):
        return PythonToDynamoDBValuesConvertor._number_handler(python_object=python_object)

    @staticmethod
    def _list_handler(python_object: list):
        for i, item in enumerate(python_object):
            python_object[i] = DynamoDBUtils.python_to_dynamodb(python_object=item)
        return {DynamoDBUtils.TYPE_LIST: python_object}

    @staticmethod
    def _dict_handler(python_object: dict):
        for key, item in python_object.items():
            python_object[key] = DynamoDBUtils.python_to_dynamodb(python_object=item)
        return {DynamoDBUtils.TYPE_MAP: python_object}

    @staticmethod
    def _bool_handler(python_object: bool):
        return {DynamoDBUtils.TYPE_BOOLEAN: python_object}

    @staticmethod
    def _str_handler(python_object: str):
        return {DynamoDBUtils.TYPE_STRING: python_object}

    @staticmethod
    def _bytes_handler(python_object: bytes):
        return {DynamoDBUtils.TYPE_BINARY: python_object}

    @staticmethod
    def _nonetype_handler(python_object: None):
        return {DynamoDBUtils.TYPE_NULL: True}

    @staticmethod
    def _default_handler(python_object: Any):
        return python_object

    @staticmethod
    def convert(python_object: Any):
        object_type: type = type(python_object)
        handler: Callable[[Any], dict] = getattr(
            PythonToDynamoDBValuesConvertor,
            f'_${object_type.__name__.lower()}_handler',
            PythonToDynamoDBValuesConvertor._default_handler
        )
        return handler(python_object)


class PythonToDynamoDBTypesConvertor:
    """
    {'NULL': True}                          None
    {'BOOL': True/False}                    True/False
    {'N': str(value)}                       Decimal(str(value))
    {'S': string}                           string
    {'B': bytes}                            Binary(bytes)
    {'NS': [str(value)]}                    set([Decimal(str(value))])
    {'SS': [string]}                        set([string])
    {'BS': [bytes]}                         set([bytes])
    {'L': list}                             list
    {'M': dict}                             dict
    """

    switch = {
        type(None): 'NULL',
        bool: 'BOOL',
        Decimal: 'N',
        int: 'N',
        float: 'N',
        str: 'S',
        bytes: 'B',
        list: 'L',
        dict: 'M'
    }

    @staticmethod
    def convert(python_type: Any) -> str:
        dynamodb_type: Optional[str] = PythonToDynamoDBTypesConvertor.switch.get(python_type, None)
        if dynamodb_type is None:
            raise Exception(message_with_vars(
                message="Python to DynamoDB types conversion failed. The specified Python type is not supported",
                vars_dict={'specifiedPythonType': python_type}
            ))
        return dynamodb_type
