def message_with_vars(message: str, vars_dict: dict):
    output_message = message
    for key, var in vars_dict.items():
        output_message += f"\n  --{key}:{var}"
    return output_message
