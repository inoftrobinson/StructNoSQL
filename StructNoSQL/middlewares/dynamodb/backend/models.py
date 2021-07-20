from dataclasses import dataclass
from typing import Optional, List, Type, Any, Dict

from pydantic import BaseModel, validate_arguments

from StructNoSQL.middlewares.dynamodb.backend.dynamodb_utils import PythonToDynamoDBTypesConvertor


HASH_KEY_TYPE = "HASH"
SORT_KEY_TYPE = "RANGE"
EXPRESSION_MAX_BYTES_SIZE = 4000  # DynamoDB max expression size is 4kb


class GetItemResponse(BaseModel):
    item: Optional[dict]
    success: bool


class Response:
    def __init__(self, response_dict: dict):
        self.items: Optional[List[dict]] = response_dict.get('Items', None)
        self.attributes: Optional[dict] = response_dict.get('Attributes', None)
        self.count: Optional[int] = response_dict.get('Count', None)
        self.scanned_count: Optional[int] = response_dict.get('ScannedCount', None)
        self.last_evaluated_key: Optional[dict] = response_dict.get('LastEvaluatedKey', None)
        self.has_reached_end = False if self.last_evaluated_key is not None else True


class Index(BaseModel):
    hash_key_name: str
    hash_key_variable_python_type: Type
    sort_key_name: Optional[str]
    sort_key_variable_python_type: Optional[Type]
    index_custom_name: Optional[str]


class PrimaryIndex(Index):
    pass


class GlobalSecondaryIndex(Index):
    # todo: add pydantic to this class
    PROJECTION_TYPE_USE_ALL = "ALL"
    PROJECTION_TYPE_KEYS_ONLY = "KEYS_ONLY"
    PROJECTION_TYPE_INCLUDE = "INCLUDE"
    ALL_PROJECTIONS_TYPES = [PROJECTION_TYPE_USE_ALL, PROJECTION_TYPE_KEYS_ONLY, PROJECTION_TYPE_INCLUDE]

    projection_type: str
    non_key_attributes: Optional[List[str]]

    def __init__(
            self, hash_key_name: str, hash_key_variable_python_type: Type,
            projection_type: str, non_key_attributes: Optional[List[str]] = None,
            sort_key_name: Optional[str] = None, sort_key_variable_python_type: Optional[Type] = None,
            index_custom_name: Optional[str] = None
    ):
        super().__init__(
            hash_key_name=hash_key_name, hash_key_variable_python_type=hash_key_variable_python_type,
            sort_index_name=sort_key_name, sort_key_variable_python_type=sort_key_variable_python_type,
            projection_type=projection_type, non_key_attributes=non_key_attributes
        )

        if projection_type not in self.ALL_PROJECTIONS_TYPES:
            raise Exception(f"{projection_type} has not been found in the available projection_types : {self.ALL_PROJECTIONS_TYPES}")
        if non_key_attributes is not None:
            if projection_type == self.PROJECTION_TYPE_INCLUDE:
                self.non_key_attributes = non_key_attributes
            else:
                raise Exception(f"In order to use non_key_attributes, you must specify the projection_type as {self.PROJECTION_TYPE_INCLUDE}")
        else:
            self.non_key_attributes = None

        self.projection_type = projection_type

    def to_dict(self):
        index_name: str = (
            self.index_custom_name if self.index_custom_name is not None
            else self.hash_key_name if self.sort_key_name is None
            else f"{self.hash_key_name}-{self.sort_key_name}"
        )
        output_dict: Dict[str, Any] = {
            "IndexName": index_name,
            "KeySchema": [
                {
                    "AttributeName": self.hash_key_name,
                    "KeyType": HASH_KEY_TYPE
                },
            ],
            "Projection": {
                "ProjectionType": self.projection_type
            }
        }
        if self.non_key_attributes is not None:
            output_dict['Projection']['NonKeyAttributes'] = self.non_key_attributes

        if self.sort_key_name is not None and self.sort_key_variable_python_type is not None:
            output_dict['KeySchema'].append({
                'AttributeName': self.sort_key_name,
                'KeyType': SORT_KEY_TYPE
            })
        return output_dict


class CreateTableQueryKwargs:
    @validate_arguments
    def __init__(self, table_name: str, billing_mode: str):
        self._names_already_defined_attributes: List[str] = []
        self.data = {
            'TableName': table_name,
            'KeySchema': [],
            'AttributeDefinitions': [],
            'BillingMode': billing_mode,
        }

    def _add_key(self, key_name: str, key_python_variable_type: Type, key_type: str):
        self.data['KeySchema'].append({
            'AttributeName': key_name,
            'KeyType': key_type
        })
        self.data['AttributeDefinitions'].append({
            'AttributeName': key_name,
            'AttributeType': PythonToDynamoDBTypesConvertor.convert(python_type=key_python_variable_type)
        })

    @validate_arguments
    def add_hash_key(self, key_name: str, key_python_variable_type: Type):
        self._add_key(key_name=key_name, key_python_variable_type=key_python_variable_type, key_type=HASH_KEY_TYPE)

    @validate_arguments
    def add_sort_key(self, key_name: str, key_python_variable_type: Type):
        self._add_key(key_name=key_name, key_python_variable_type=key_python_variable_type, key_type=SORT_KEY_TYPE)

    def _add_global_secondary_index(self, key_name: str, key_python_variable_type: Type):
        if key_name not in self._names_already_defined_attributes:
            self.data["AttributeDefinitions"].append({
                "AttributeName": key_name,
                "AttributeType": PythonToDynamoDBTypesConvertor.convert(python_type=key_python_variable_type)
            })
            self._names_already_defined_attributes.append(key_name)

    @validate_arguments
    def add_global_secondary_index(self, global_secondary_index: GlobalSecondaryIndex):
        if 'GlobalSecondaryIndexes' not in self.data:
            self.data['GlobalSecondaryIndexes'] = []

        self._add_global_secondary_index(
            key_name=global_secondary_index.hash_key_name,
            key_python_variable_type=global_secondary_index.hash_key_variable_python_type
        )
        if global_secondary_index.sort_key_name is not None and global_secondary_index.sort_key_variable_python_type is not None:
            self._add_global_secondary_index(
                key_name=global_secondary_index.sort_key_name,
                key_python_variable_type=global_secondary_index.sort_key_variable_python_type
            )
        self.data["GlobalSecondaryIndexes"].append(global_secondary_index.to_dict())

    @validate_arguments
    def add_all_global_secondary_indexes(self, global_secondary_indexes: List[GlobalSecondaryIndex]):
        for global_secondary_index in global_secondary_indexes:
            self.add_global_secondary_index(global_secondary_index=global_secondary_index)
