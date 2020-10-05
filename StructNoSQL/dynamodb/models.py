from dataclasses import dataclass


@dataclass
class DatabasePathElement:
    element_key: str
    default_type: type

