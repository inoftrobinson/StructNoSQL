def remove_null_elements_from_dict(dict_instance: dict) -> dict:
    output_clean_dict = dict()
    for key, value in dict_instance.items():
        if isinstance(value, dict):
            nested = remove_null_elements_from_dict(value)
            if len(nested.keys()) > 0:
                output_clean_dict[key] = nested
        elif value is not None:
            output_clean_dict[key] = value
    return output_clean_dict
