from typing import Any

# todo: finish a correct float to decimal convertor, because currently, the float_to_decimal that
#  simply convert the float to a string and load it into a decimal, is limited to 18 decimals
#  instead of 38, because the string representation of a float will be rounded after 18 decimals.

"""
from decimal import Context, ROUND_HALF_EVEN, Decimal, getcontext

DECIMAL_DYNAMODB_CONTEXT = Context(
    prec=38, rounding=ROUND_HALF_EVEN,
    Emin=-128, Emax=126,
    capitals=1, clamp=0,
    flags=[], traps=[]
)

def float_to_decimal(float_number: float):
    split_float = str(float_number).split(".")
    if len(split_float) > 1:
        num_decimals = len(split_float[1])
        if num_decimals <= 38:
            return Decimal(f"{float_number}")
        else:
            return Decimal(f"{round(float_number, 38)}")
    else:
        return Decimal(f"{float_number}")
"""


from decimal import Decimal


def float_to_decimal(float_number: float) -> Decimal:
    return Decimal(f"{float_number}")

def float_to_decimal_serializer(item: Any) -> Any:
    if isinstance(item, dict):
        for key, value in item.items():
            item[key] = float_to_decimal_serializer(item=value)
    elif isinstance(item, list):
        for i, value in enumerate(item):
            item[i] = float_to_decimal_serializer(item=value)
    elif isinstance(item, float):
        item = float_to_decimal(float_number=item)
    return item


