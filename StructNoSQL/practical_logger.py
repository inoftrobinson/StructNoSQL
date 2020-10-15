from pprint import pformat


def message_with_vars(message: str, vars_dict: dict):
    output_message = f"\n{message}"
    for var_key, var_value in vars_dict.items():
        output_message += f"\n  --{var_key}:{pformat(var_value)}"
    return output_message


