from pprint import pformat
from typing import Optional


def message_with_vars(message: str, vars_dict: Optional[dict] = None) -> str:
    output_message = f"\n{message}"
    if vars_dict is not None:
        for var_key, var_value in vars_dict.items():
            output_message += f"\n  --{var_key}:{pformat(var_value)}"
    return output_message


