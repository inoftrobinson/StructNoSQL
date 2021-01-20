class DataValidationException(Exception):
    def __init__(self, message: str):
        super().__init__(message)

class FieldTargetNotFoundException(Exception):
    pass

class InvalidFieldNameException(Exception):
    pass

class UsageOfUntypedSetException(Exception):
    def __init__(self):
        super().__init__(
            "Cannot use an untyped set as field_type. You must use a typed Set with a single primitive type."
            "\nFirst, import the Set object like so : \nfrom typing import Set"
            "\nThen replace the usage of 'set' as field_type by : Set[str] or Set[int] or Set[float] or Set[bool] or Set[bytes]"
            "\nWe do that, because in DynamoDB a set can only contain items of the same type. If StructNoSQL did not imposed to"
            "\nuse typed Set's instead of classic set, a crashing request that will be denied by DynamoDB could be send by"
            "\nStructNoSQL if you tried to add items of different type inside the same set.",
        )
