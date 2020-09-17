import boto3
from boto3.dynamodb.conditions import Key, Attr
from boto3.exceptions import ResourceNotExistsError
from boto3.session import Session
from typing import List, Optional, Type, Any, Dict

from botocore.exceptions import ClientError
from pydantic import BaseModel, validate_arguments
from pydantic.dataclasses import dataclass

from StructNoSQL.dynamodb.dynamodb_utils import Utils
from StructNoSQL.safe_dict import SafeDict
from StructNoSQL.utils.static_logger import logger

HASH_KEY_TYPE = "HASH"
SORT_KEY_TYPE = "RANGE"


class GetItemResponse(BaseModel):
    item: Optional[dict]
    success: bool

class Response:
    def __init__(self, response_dict: dict):
        response_safedict = SafeDict(response_dict)
        self.items = response_safedict.get("Items").to_list(default=None)
        self.count = response_safedict.get("Count").to_int(default=None)
        self.scanned_count = response_safedict.get("ScannedCount").to_int(default=None)
        self.last_evaluated_key = response_safedict.get("LastEvaluatedKey").to_dict(default=None)
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

    def __init__(self, projection_type: str, hash_key_name: str, hash_key_variable_python_type: Type,
                 sort_key_name: Optional[str] = None, sort_key_variable_python_type: Optional[Type] = None,
                 index_custom_name: Optional[str] = None):

        super().__init__(hash_key_name=hash_key_name, hash_key_variable_python_type=hash_key_variable_python_type,
                         sort_key_name=sort_key_name, sort_key_variable_python_type=sort_key_variable_python_type,
                         projection_type=projection_type)

        if projection_type not in self.ALL_PROJECTIONS_TYPES:
            raise Exception(f"{projection_type} has not been found in the available projection_types : {self.ALL_PROJECTIONS_TYPES}")
        self.projection_type = projection_type

    def to_dict(self):
        if self.index_custom_name is not None:
            index_name = self.index_custom_name
        else:
            if self.sort_key_name is None:
                index_name = self.hash_key_name
            else:
                index_name = f"{self.hash_key_name}-{self.sort_key_name}"

        output_dict = {
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
        if self.sort_key_name is not None and self.sort_key_variable_python_type is not None:
            output_dict["KeySchema"].append({
                "AttributeName": self.sort_key_name,
                "KeyType": SORT_KEY_TYPE
            })
        return output_dict


class CreateTableQueryKwargs:
    @validate_arguments
    def __init__(self, table_name: str, billing_mode: str):
        self._names_already_defined_attributes: List[str] = list()
        self.data = {
            "TableName": table_name,
            "KeySchema": list(),
            "AttributeDefinitions": list(),
            "BillingMode": billing_mode,
        }

    def _add_key(self, key_name: str, key_python_variable_type: Type, key_type: str):
        self.data["KeySchema"].append({
            "AttributeName": key_name,
            "KeyType": key_type
        })
        self.data["AttributeDefinitions"].append({
            "AttributeName": key_name,
            "AttributeType": Utils.python_type_to_dynamodb_type(python_type=key_python_variable_type)
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
                "AttributeType": Utils.python_type_to_dynamodb_type(python_type=key_python_variable_type)
            })
            self._names_already_defined_attributes.append(key_name)

    @validate_arguments
    def add_global_secondary_index(self, global_secondary_index: GlobalSecondaryIndex):
        if "GlobalSecondaryIndexes" not in self.data:
            self.data["GlobalSecondaryIndexes"] = list()

        self._add_global_secondary_index(key_name=global_secondary_index.hash_key_name,
                                         key_python_variable_type=global_secondary_index.hash_key_variable_python_type)
        if global_secondary_index.sort_key_name is not None and global_secondary_index.sort_key_variable_python_type is not None:
            self._add_global_secondary_index(key_name=global_secondary_index.sort_key_name,
                                             key_python_variable_type=global_secondary_index.sort_key_variable_python_type)

        self.data["GlobalSecondaryIndexes"].append(global_secondary_index.to_dict())

    @validate_arguments
    def add_all_global_secondary_indexes(self, global_secondary_indexes: List[GlobalSecondaryIndex]):
        for global_secondary_index in global_secondary_indexes:
            self.add_global_secondary_index(global_secondary_index=global_secondary_index)


@dataclass
class DynamoDBMapObjectSetter:
    object_path: str
    value_to_set: Any
    map_key: Optional[str] = None


class DynamoDbCoreAdapter:
    _EXISTING_DATABASE_CLIENTS = dict()
    PAY_PER_REQUEST = "PAY_PER_REQUEST"
    PROVISIONED = "PROVISIONED"

    def __init__(self, table_name: str, region_name: str, primary_index: PrimaryIndex,
                 create_table: bool = True, billing_mode: str = PAY_PER_REQUEST,
                 global_secondary_indexes: List[GlobalSecondaryIndex] = None):
        self.table_name = table_name
        self.primary_index = primary_index
        self.create_table = create_table
        self.billing_mode = billing_mode
        self.global_secondary_indexes = global_secondary_indexes

        # We store the database clients in a static variable, so that if we init the class with
        # the same region_name, we do not need to wait for a new initialization of the client.
        if region_name in self._EXISTING_DATABASE_CLIENTS.keys():
            self.dynamodb = self._EXISTING_DATABASE_CLIENTS[region_name]
            print(f"Re-using the already created dynamodb client for region {region_name}")
        elif "default" in self._EXISTING_DATABASE_CLIENTS.keys():
            self.dynamodb = self._EXISTING_DATABASE_CLIENTS["default"]
            print(f"Re-using the already created dynamodb client for the default region")
        else:
            print(f"Initializing the {self}. For local development, make sure that you are connected to internet."
                  f"\nOtherwise the framework will get stuck at initializing the {self}")

            dynamodb_regions = Session().get_available_regions("dynamodb")
            if region_name in dynamodb_regions:
                self.dynamodb = boto3.resource("dynamodb", region_name=region_name)
                self._EXISTING_DATABASE_CLIENTS[region_name] = self.dynamodb
            else:
                self.dynamodb = boto3.resource("dynamodb")
                self._EXISTING_DATABASE_CLIENTS["default"] = self.dynamodb
                logger.debug(f"Warning ! The specified dynamodb region_name {region_name} is not a valid region_name."
                             f"The dynamodb client has been initialized without specifying the region.")

        self._create_table_if_not_exists()
        print(f"Initialization of {self} completed successfully !")

    def _create_table_if_not_exists(self) -> None:
        """
        Creates table in Dynamodb resource if it doesn't exist and create_table is set as True.
        :raises: PersistenceException: When `create_table` fails on dynamodb resource.
        """
        if self.create_table:
            create_table_query_kwargs = CreateTableQueryKwargs(table_name=self.table_name, billing_mode=self.billing_mode)

            create_table_query_kwargs.add_hash_key(key_name=self.primary_index.hash_key_name,
                                                   key_python_variable_type=self.primary_index.hash_key_variable_python_type)

            if self.primary_index.sort_key_name is not None and self.primary_index.sort_key_variable_python_type is not None:
                create_table_query_kwargs.add_hash_key(key_name=self.primary_index.sort_key_name,
                                                       key_python_variable_type=self.primary_index.sort_key_variable_python_type)

            if self.global_secondary_indexes is not None:
                create_table_query_kwargs.add_all_global_secondary_indexes(global_secondary_indexes=self.global_secondary_indexes)
            try:
                print(create_table_query_kwargs.data)
                self.dynamodb.create_table(**create_table_query_kwargs.data)
            except Exception as e:
                if e.__class__.__name__ != "ResourceInUseException":
                    raise Exception(f"Create table if not exists request failed: Exception of type {type(e).__name__} occurred {str(e)}")

    def put_item_dict(self, item_dict: dict) -> Response:
        try:
            table = self.dynamodb.Table(self.table_name)
            response = table.put_item(Item=item_dict)
            return response
        except ResourceNotExistsError:
            raise Exception(f"DynamoDb table {self.table_name} doesn't exist. Failed to save attributes to DynamoDb table.")
        except Exception as e:
            raise Exception(f"Failed to save attributes to DynamoDb table. Exception of type {type(e).__name__} occurred: {str(e)}")

    def get_item_by_primary_key(self, key_name: str, key_value: any, fields_to_get: Optional[List[str]]) -> Optional[GetItemResponse]:
        if fields_to_get is not None:
            kwargs = self._fields_to_get_to_expressions(fields_to_get=fields_to_get)
        else:
            kwargs = dict()
        kwargs["Key"] = {key_name: key_value}
        kwargs["ConsistentRead"] = True

        try:
            table = self.dynamodb.Table(self.table_name)
            response = table.get_item(**kwargs)
            if "Item" in response:
                return GetItemResponse(item=Utils.dynamodb_to_python(dynamodb_object=response["Item"]), success=True)
            else:
                return GetItemResponse(item=None, success=False)
        except ResourceNotExistsError:
            raise Exception(f"DynamoDb table {self.table_name} do not exist or in the process"
                            "of being created. Failed to get attributes from DynamoDb table.")
        except Exception as e:
            raise Exception(f"Failed to retrieve attributes from DynamoDb table."
                            f"Exception of type {type(e).__name__} occurred: {str(e)}")

    def _execute_update_query(self, query_kwargs_dict: dict) -> Optional[Response]:
        try:
            table = self.dynamodb.Table(self.table_name)
            response = table.update_item(**query_kwargs_dict)
            return Response(response)
        except ResourceNotExistsError:
            raise Exception(f"DynamoDb table {self.table_name} do not exist or in the process"
                            "of being created. Failed to get attributes from DynamoDb table.")
        except ClientError as e:
            print(f"{e} - No element has been found for the update query : {query_kwargs_dict}")
            return None
        except Exception as e:
            print(type(e))
            raise Exception(f"Failed to update attributes in DynamoDb table."
                            f"Exception of type {type(e).__name__} occurred: {str(e)}")

    def add_data_elements_to_list(self, key_name: str, key_value: Any, object_path: str,
                                  element_values: List[dict]) -> Optional[Response]:
        kwargs = {
            "TableName": self.table_name,
            "Key": {key_name: key_value},
            "ReturnValues": "UPDATED_NEW",
            "UpdateExpression": f"SET {object_path} = list_append(if_not_exists({object_path}, :emptyList), :newItems)",
            # The if_not_exists inside the list_append, will create an empty
            # list before adding the newItems, only if the field do not exist.
            "ExpressionAttributeValues": {
                ":newItems": element_values,
                ":emptyList": []
            }
        }
        return self._execute_update_query(query_kwargs_dict=kwargs)

    def remove_data_elements_from_list(self, key_name: str, key_value: Any, list_object_path: str,
                                       indexes_to_remove: list) -> Optional[Response]:
        kwargs = {
            "TableName": self.table_name,
            "Key": {key_name: key_value},
            "ReturnValues": "UPDATED_NEW"
        }
        update_expression = "REMOVE "
        for i, index_in_database_list in enumerate(indexes_to_remove):
            update_expression += f"{list_object_path}[{index_in_database_list}]"
            if i + 1 < len(indexes_to_remove):
                update_expression += ", "
        kwargs["UpdateExpression"] = update_expression

        return self._execute_update_query(query_kwargs_dict=kwargs)

    def initialize_all_elements_in_map_target(self, key_name: str, key_value: Any, object_path_elements: Dict[str, type]) -> bool:
        current_path_target = ""
        expression_attribute_names_dict: Dict[str, str] = dict()
        for i, path_element_key in enumerate(object_path_elements):
            if i > 0:
                current_path_target += "."

            current_path_key = f"#pathKey{i}"
            current_path_target += current_path_key
            expression_attribute_names_dict[current_path_key] = path_element_key

            current_update_expression = f"SET {current_path_target} = if_not_exists({current_path_target}, :item)"
            current_path_initialization_variable_type: type = object_path_elements[path_element_key]
            current_set_potentially_missing_object_query_kwargs = {
                "TableName": self.table_name,
                "Key": {key_name: key_value},
                "ReturnValues": "UPDATED_NEW",
                "UpdateExpression": current_update_expression,
                "ExpressionAttributeNames": expression_attribute_names_dict,
                "ExpressionAttributeValues": {
                    ":item": current_path_initialization_variable_type()
                }
            }
            response = self._execute_update_query(query_kwargs_dict=current_set_potentially_missing_object_query_kwargs)
            if response is None:
                return False
        return True

    def add_update_data_element_to_map(self, key_name: str, key_value: Any,
                                       object_path_elements: Dict[str, type], element_values: Any) -> Optional[Response]:
        expression_attribute_names_dict = dict()
        update_expression = "SET "

        for i, path_element in enumerate(object_path_elements):
            current_path_key = f"#pathKey{i}"
            update_expression += current_path_key
            expression_attribute_names_dict[current_path_key] = path_element
            if i + 1 < len(object_path_elements):
                update_expression += "."
            else:
                update_expression += " = :item"

        update_query_kwargs = {
            "TableName": self.table_name,
            "Key": {key_name: key_value},
            "ReturnValues": "UPDATED_NEW",
            "UpdateExpression": update_expression,
            "ExpressionAttributeNames": expression_attribute_names_dict,
            "ExpressionAttributeValues": {
                ":item": element_values
            }
        }
        response = self._execute_update_query(query_kwargs_dict=update_query_kwargs)
        if response is None:
            # If the response is None, it means that one of the path of the
            # target path has not been found and need to be initialized.
            success: bool = self.initialize_all_elements_in_map_target(
                key_name=key_name, key_value=key_value, object_path_elements=object_path_elements
            )
            if success is True:
                response = self._execute_update_query(query_kwargs_dict=update_query_kwargs)
        return response

    def add_update_multiple_data_elements_to_map(self, key_name: str, key_value: Any,
                                                 objects_setters: List[DynamoDBMapObjectSetter]) -> Optional[Response]:
        if not len(objects_setters) > 0:
            # If no object setter has been defined,
            # the query will crash when executed.
            return Response({})

        kwargs = {
            "TableName": self.table_name,
            "Key": {key_name: key_value},
            "ReturnValues": "UPDATED_NEW"
        }

        update_expression = "SET "
        attribute_names_expression_dict = dict()
        attribute_values_expression_dict = dict()
        for i, current_object in enumerate(objects_setters):
            # We use the attribute_names and attribute_values expressions instead of putting the data
            # directly in the update_expression, because boto3 will only convert the data in the
            # appropriate format when it is put in the attribute and values expression.
            # It will not convert it if the data is directly set in the update expression.
            update_expression += f"{current_object.object_path}"

            if current_object.map_key is not None:
                update_expression += f".#key{i}"
                attribute_names_expression_dict[f"#key{i}"] = current_object.map_key

            update_expression += f" = :item{i}"
            attribute_values_expression_dict[f":item{i}"] = current_object.value_to_set

            if i + 1 < len(objects_setters):
                update_expression += ", "

        kwargs["UpdateExpression"] = update_expression
        kwargs["ExpressionAttributeValues"] = attribute_values_expression_dict
        if len(attribute_names_expression_dict) > 0:
            kwargs["ExpressionAttributeNames"] = attribute_names_expression_dict

        return self._execute_update_query(query_kwargs_dict=kwargs)

    def query_by_key(self, key_name: str, key_value: Any, index_name: Optional[str] = None,
                     fields_to_get: Optional[list] = None, filter_expression: Optional[Any] = None,
                     query_limit: Optional[int] = None) -> Response:

        if fields_to_get is not None:
            kwargs = self._fields_to_get_to_expressions(fields_to_get=fields_to_get)
        else:
            kwargs = dict()

        kwargs["KeyConditionExpression"] = Key(key_name).eq(key_value)
        if index_name is not None:
            kwargs["IndexName"] = index_name

        if filter_expression is not None:
            kwargs["FilterExpression"] = filter_expression

        if query_limit is not None:
            kwargs["Limit"] = query_limit

        try:
            table = self.dynamodb.Table(self.table_name)
            response = table.query(**kwargs)
            return Response(Utils.dynamodb_to_python(response))
        except ResourceNotExistsError:
            raise Exception(f"DynamoDb table {self.table_name} do not exist or in the process"
                            "of being created. Failed to get attributes from DynamoDb table.")
        except Exception as e:
            raise Exception(f"Failed to retrieve attributes from DynamoDb table."
                            f"Exception of type {type(e).__name__} occurred: {str(e)}")

    def query_single_item_by_key(self, key_name: str, key_value: Any, index_name: Optional[str] = None,
                                 fields_to_get: Optional[list] = None, filter_expression: Optional[Any] = None) -> Optional[dict]:
        # Yes, a query request is heavier than a get request that we could do with the _get_item_by_primary_key function.
        # Yet, in a get request, we cannot specify an index_name to query on. So, the _query_single_item_by_key should be
        # used when we want to get an item based on another index that the primary one. Otherwise, use _get_item_by_primary_key
        response = self.query_by_key(
            key_name=key_name, key_value=key_value, index_name=index_name,
            fields_to_get=fields_to_get, filter_expression=filter_expression, query_limit=1
        )
        if response.count == 1:
            return response.items[0]
        elif not response.count > 0:
            print("No item has been found")
            return None
        elif not response.count > 1:
            print("More than one item has been found. Returning first item.")
            return response.items[0]

    def get_data_in_path_target(self, key_name: str, key_value: str, target_path_elements: Dict[str, type],
                                 num_keys_to_navigation_into: int) -> Optional[any]:
        target_field = ""
        for i, key_path_element in enumerate(target_path_elements):
            if i > 0:
                target_field += "."
            target_field += key_path_element

        response_item: Optional[dict] = self.get_item_by_primary_key(
            key_name=key_name, key_value=key_value, fields_to_get=[target_field]
        ).item
        for i, key_path_element in enumerate(target_path_elements):
            if i + 1 > num_keys_to_navigation_into:
                break

            response_item = response_item.get(key_path_element, None)
            if response_item is None:
                # If the response_item is None, it means that one key has not been found,
                # so we need to break the loop in order to try to call get on a None object,
                # and then we will return the response_item, so we will return None.
                break
        return response_item

    def get_value_in_path_target(self, key_name: str, key_value: str, target_path_elements: Dict[str, type]) -> Optional[any]:
        return self.get_data_in_path_target(
            key_name=key_name, key_value=key_value,
            target_path_elements=target_path_elements,
            num_keys_to_navigation_into=len(target_path_elements)
        )

    def get_item_in_path_target(self, key_name: str, key_value: str, target_path_elements: Dict[str, type]) -> Optional[dict]:
        return self.get_data_in_path_target(
            key_name=key_name, key_value=key_value,
            target_path_elements=target_path_elements,
            num_keys_to_navigation_into=len(target_path_elements) - 1
        )

    @staticmethod
    def _add_to_filter_expression(expression, condition):
        if expression is None:
            return condition
        return expression & condition

    @staticmethod
    def _fields_to_get_to_expressions(fields_to_get: List[str]) -> dict:
        output_kwargs = {}
        expression_attribute_names = {}
        projection_expression = ""

        # In DynamoDB, when trying to get some fields, certain type of values (like values with - in them,
        # or big numbers like an UUID), can not work and provoke in error while executing the query. In
        # order to fix that, we need to pass the variable name in the ExpressionAttributeNames instead of
        # putting it directly in the ProjectionExpression. Yet, when using a map path like myMap.data.item1
        # we must declare each path element (myMap, data, item1) as separate ExpressionAttributeNames,
        # otherwise it will not work. So, we define an 'id' for each path of each attribute name
        # (f"#f{i_field}_{i_path}") we add our path as the dict value, and then we build the condition
        # expression to use the ids of our attributes names.
        for i_field, field in enumerate(fields_to_get):
            field: str  # PyCharm did not recognized the field variable as a string ;)
            path_elements = field.split(".")

            for i_path, path in enumerate(path_elements):
                current_field_path_expression_name = f"#f{i_field}_{i_path}"
                expression_attribute_names[current_field_path_expression_name] = path
                projection_expression += current_field_path_expression_name
                if i_path + 1 < len(path_elements):
                    projection_expression += "."

            if i_field + 1 < len(fields_to_get):
                projection_expression += ", "

        if len(expression_attribute_names) > 0:
            output_kwargs["ExpressionAttributeNames"] = expression_attribute_names
        if projection_expression.replace(" ", "") != "":
            output_kwargs["ProjectionExpression"] = projection_expression

        return output_kwargs


