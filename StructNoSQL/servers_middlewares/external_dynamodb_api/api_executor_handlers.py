from typing import List, Dict, Any, Optional, Tuple

from StructNoSQL.clients_middlewares.dynamodb.backend.dynamodb_core import Response

from StructNoSQL.models import DatabasePathElement, FieldPathSetter
from pydantic import BaseModel

from StructNoSQL.servers_middlewares.external_dynamodb_api.low_level_table_client import DynamoDBLowLevelTableClient
from StructNoSQL.servers_middlewares.external_dynamodb_api.models import FieldPathElementItemModel


def putRecord(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        recordItemData: dict

    request_data = RequestDataModel(**data)
    success: bool = table.dynamodb_client.put_record(
        item_dict=request_data.recordItemData
    )
    return success, None, {}


def deleteRecord(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        indexesKeysSelectors: Dict[str, Any]

    request_data = RequestDataModel(**data)
    record_deletion_success: bool = table.dynamodb_client.delete_record(
        indexes_keys_selectors=request_data.indexesKeysSelectors
    )
    return record_deletion_success, None, {}


def removeRecord(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        indexesKeysSelectors: Dict[str, Any]

    request_data = RequestDataModel(**data)
    removed_record_data: Optional[dict] = table.dynamodb_client.remove_record(
        indexes_keys_selectors=request_data.indexesKeysSelectors
    )
    return (True, removed_record_data, {}) if removed_record_data is not None else (False, {}, {})


def queryItemsByKey(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        keyValue: str
        fieldsPathElements: List[List[FieldPathElementItemModel]]
        paginationRecordsLimit: Optional[int] = None
        filterExpression: Optional[Any] = None

    request_data = RequestDataModel(**data)
    fields_path_elements: List[List[DatabasePathElement]] = [
        [
            DatabasePathElement(
                element_key=item.elementKey,
                default_type=item.defaultType,
                custom_default_value=item.customDefaultValue
            )
            for item in path_elements
        ]
        for path_elements in request_data.fieldsPathElements
    ]
    records_attributes, query_metadata = table.dynamodb_client.query_items_by_key(
        index_name=table.primary_index_name, key_value=request_data.keyValue, fields_path_elements=fields_path_elements,
        pagination_records_limit=request_data.paginationRecordsLimit, filter_expression=request_data.filterExpression
    )
    return True, {'data': records_attributes, 'metadata': query_metadata.serialize()}, {}


def getOrQuerySingleItem(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        keyValue: str
        fieldsPathElements: List[List[FieldPathElementItemModel]]

    request_data = RequestDataModel(**data)
    fields_path_elements: List[List[DatabasePathElement]] = [
        [
            DatabasePathElement(
                element_key=item.elementKey,
                default_type=item.defaultType,
                custom_default_value=item.customDefaultValue
            )
            for item in path_elements_container
        ]
        for path_elements_container in request_data.fieldsPathElements
    ]
    response_data = table.dynamodb_client.get_or_query_single_item(
        index_name=table.primary_index_name, key_value=request_data.keyValue,
        fields_path_elements=fields_path_elements
    )
    return True, response_data, {}


def getSingleValueInPathTarget(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        keyValue: str
        fieldPathElements: List[FieldPathElementItemModel]

    request_data = RequestDataModel(**data)
    field_path_elements: List[DatabasePathElement] = [
        DatabasePathElement(
            element_key=item.elementKey,
            default_type=item.defaultType,
            custom_default_value=item.customDefaultValue
        )
        for item in request_data.fieldPathElements
    ]
    response_data = table.dynamodb_client.get_value_in_path_target(
        index_name=table.primary_index_name, key_value=request_data.keyValue,
        field_path_elements=field_path_elements
    )
    return True, response_data, {}


def getValuesInMultiplePathTarget(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        keyValue: str
        fieldsPathElements: Dict[str, List[FieldPathElementItemModel]]

    request_data = RequestDataModel(**data)
    fields_path_elements: Dict[str, List[DatabasePathElement]] = {
        key: [
            DatabasePathElement(
                element_key=item.elementKey,
                default_type=item.defaultType,
                custom_default_value=item.customDefaultValue
            )
            for item in path_elements_container
        ]
        for key, path_elements_container in request_data.fieldsPathElements.items()
    }
    response_data = table.dynamodb_client.get_values_in_multiple_path_target(
        index_name=table.primary_index_name, key_value=request_data.keyValue,
        fields_path_elements=fields_path_elements, metadata=False
    )
    return (True, response_data, {}) if response_data is not None else (False, {}, {})

def setUpdateMultipleDataElementsToMap(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        keyValue: str
        class SetterItemModel(BaseModel):
            valueToSet: Any
            fieldPathElements: List[FieldPathElementItemModel]
        setters: List[SetterItemModel]
        returnOldValues: bool = False

    request_data = RequestDataModel(**data)
    setters: List[FieldPathSetter] = [
        FieldPathSetter(
            value_to_set=setter_item.valueToSet,
            field_path_elements=[
                DatabasePathElement(
                    element_key=item.elementKey,
                    default_type=item.defaultType,
                    custom_default_value=item.customDefaultValue
                )
                for item in setter_item.fieldPathElements
            ]
        )
        for setter_item in request_data.setters
    ]
    response: Optional[Response] = table.dynamodb_client.set_update_multiple_data_elements_to_map(
        index_name=table.primary_index_name, key_value=request_data.keyValue,
        setters=setters, return_old_values=request_data.returnOldValues
    )
    if response is None:
        return False, None, {}

    from StructNoSQL.clients_middlewares.dynamodb.backend.dynamodb_utils import DynamoDBUtils
    response_attributes: Optional[dict] = (
        DynamoDBUtils.dynamodb_to_python_higher_level(response.attributes)
        if response.attributes is not None else None
    )
    return True, response_attributes, {}

def setUpdateMultipleDataElementsToMapWithDefaultInitialization(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        keyValue: str
        fieldPathElements: List[FieldPathElementItemModel]
        value: Any
        returnOldValue: bool = False

    request_data = RequestDataModel(**data)
    field_path_elements: List[DatabasePathElement] = [
        DatabasePathElement(
            element_key=item.elementKey,
            default_type=item.defaultType,
            custom_default_value=item.customDefaultValue
        )
        for item in request_data.fieldPathElements
    ]
    response: Optional[Response] = table.dynamodb_client.set_update_data_element_to_map_with_default_initialization(
        index_name=table.primary_index_name, key_value=request_data.keyValue, field_path_elements=field_path_elements,
        return_old_value=request_data.returnOldValue, value=request_data.value,
    )
    if response is None:
        return False, None, {}

    from StructNoSQL.clients_middlewares.dynamodb.backend.dynamodb_utils import DynamoDBUtils
    response_attributes: Optional[Dict[str, Any]] = (
        DynamoDBUtils.dynamodb_to_python_higher_level(response.attributes)
        if response.attributes is not None else None
    )
    return True, response_attributes, {}

def removeDataElementsFromMap(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        keyValue: str
        fieldsPathElements: List[List[FieldPathElementItemModel]]

    request_data = RequestDataModel(**data)
    fields_path_elements: List[List[DatabasePathElement]] = [
        [
            DatabasePathElement(
                element_key=item.elementKey,
                default_type=item.defaultType,
                custom_default_value=item.customDefaultValue
            )
            for item in path_elements_container
        ]
        for path_elements_container in request_data.fieldsPathElements
    ]
    response_data = table.dynamodb_client.remove_data_elements_from_map(
        index_name=table.primary_index_name, key_value=request_data.keyValue,
        targets_path_elements=fields_path_elements,
        retrieve_removed_elements=True
    )
    return (True, response_data, {}) if response_data is not None else (False, {}, {})

def deleteDataElementsFromMap(table: DynamoDBLowLevelTableClient, data: dict) -> Tuple[bool, Optional[dict], dict]:
    class RequestDataModel(BaseModel):
        keyValue: str
        fieldsPathElements: List[List[FieldPathElementItemModel]]

    request_data = RequestDataModel(**data)
    fields_path_elements: List[List[DatabasePathElement]] = [
        [
            DatabasePathElement(
                element_key=item.elementKey,
                default_type=item.defaultType,
                custom_default_value=item.customDefaultValue
            )
            for item in path_elements_container
        ]
        for path_elements_container in request_data.fieldsPathElements
    ]
    response_data = table.dynamodb_client.remove_data_elements_from_map(
        index_name=table.primary_index_name, key_value=request_data.keyValue,
        targets_path_elements=fields_path_elements,
        retrieve_removed_elements=False
    )
    return True if response_data is not None else False, {}, {}
