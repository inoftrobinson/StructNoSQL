import logging
from sys import getsizeof
from concurrent.futures.thread import ThreadPoolExecutor

import boto3
from boto3.dynamodb.conditions import Key
from boto3.exceptions import ResourceNotExistsError
from boto3.session import Session
from typing import List, Optional, Type, Any, Dict, Tuple

from botocore.exceptions import ClientError
from pydantic import BaseModel, validate_arguments

from StructNoSQL.middlewares.dynamodb.backend.dynamodb_utils import DynamoDBUtils, PythonToDynamoDBTypesConvertor
from StructNoSQL.models import DatabasePathElement, DynamoDBMapObjectSetter, MapItemInitializer, \
    MapItemInitializerContainer
from StructNoSQL.practical_logger import message_with_vars

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

    def __init__(self, hash_key_name: str, hash_key_variable_python_type: Type,
                 projection_type: str, non_key_attributes: Optional[List[str]] = None,
                 sort_key_name: Optional[str] = None, sort_key_variable_python_type: Optional[Type] = None,
                 index_custom_name: Optional[str] = None):

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
        if self.non_key_attributes is not None:
            output_dict["Projection"]["NonKeyAttributes"] = self.non_key_attributes

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
            "AttributeType": PythonToDynamoDBTypesConvertor.convert(python_type=key_python_variable_type)
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
        self._global_secondary_indexes_hash_keys = list()
        if self.global_secondary_indexes is not None:
            for secondary_index in self.global_secondary_indexes:
                self._global_secondary_indexes_hash_keys.append(secondary_index.hash_key_name)

        # We store the database clients in a static variable, so that if we init the class with
        # the same region_name, we do not need to wait for a new initialization of the client.
        if region_name in self._EXISTING_DATABASE_CLIENTS.keys():
            self.dynamodb = self._EXISTING_DATABASE_CLIENTS[region_name]
            # print(f"Re-using the already created dynamodb client for region {region_name}")
        elif "default" in self._EXISTING_DATABASE_CLIENTS.keys():
            self.dynamodb = self._EXISTING_DATABASE_CLIENTS["default"]
            # print(f"Re-using the already created dynamodb client for the default region")
        else:
            # print(f"Initializing the {self}. For local development, make sure that you are connected to internet."
            #       f"\nOtherwise the DynamoDB client will get stuck at initializing the {self}")

            dynamodb_regions = Session().get_available_regions("dynamodb")
            if region_name in dynamodb_regions:
                self.dynamodb = boto3.resource("dynamodb", region_name=region_name)
                self._EXISTING_DATABASE_CLIENTS[region_name] = self.dynamodb
            else:
                self.dynamodb = boto3.resource("dynamodb")
                self._EXISTING_DATABASE_CLIENTS["default"] = self.dynamodb
                logging.debug(f"Warning ! The specified dynamodb region_name {region_name} is not a valid region_name."
                              f"The dynamodb client has been initialized without specifying the region.")

        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self) -> None:
        """
        Creates table in Dynamodb resource if it doesn't exist and create_table is set as True.
        :raises: PersistenceException: When `create_table` fails on dynamodb resource.
        """
        if self.create_table:
            create_table_query_kwargs = CreateTableQueryKwargs(table_name=self.table_name, billing_mode=self.billing_mode)
            create_table_query_kwargs.add_hash_key(
                key_name=self.primary_index.hash_key_name,
                key_python_variable_type=self.primary_index.hash_key_variable_python_type
            )
            if self.primary_index.sort_key_name is not None and self.primary_index.sort_key_variable_python_type is not None:
                create_table_query_kwargs.add_hash_key(
                    key_name=self.primary_index.sort_key_name,
                    key_python_variable_type=self.primary_index.sort_key_variable_python_type
                )

            if self.global_secondary_indexes is not None:
                create_table_query_kwargs.add_all_global_secondary_indexes(global_secondary_indexes=self.global_secondary_indexes)
            try:
                self.dynamodb.create_table(**create_table_query_kwargs.data)
            except Exception as e:
                if e.__class__.__name__ != "ResourceInUseException":
                    raise Exception(f"Request to create the table if not exist failed: Exception of type {type(e).__name__} occurred {str(e)}")

    def put_record(self, item_dict: dict) -> bool:
        try:
            table = self.dynamodb.Table(self.table_name)
            response = table.put_item(Item=item_dict)
            return True if response is not None else False
        except ResourceNotExistsError:
            raise Exception(f"DynamoDb table {self.table_name} doesn't exist. Failed to save attributes to DynamoDb table.")
        except Exception as e:
            print(f"Failed to save attributes to DynamoDb table. Exception of type {type(e).__name__} occurred: {str(e)}")
        return False

    def delete_record(self, indexes_keys_selectors: dict) -> bool:
        try:
            table = self.dynamodb.Table(self.table_name)
            response = table.delete_item(Key=indexes_keys_selectors)
            return True if response is not None else False
        except ResourceNotExistsError:
            raise Exception(f"DynamoDb table {self.table_name} doesn't exist. Failed to save attributes to DynamoDb table.")
        except Exception as e:
            print(e)
        return False

    def get_item_by_primary_key(self, index_name: str, key_value: any, fields_path_elements: Optional[List[List[DatabasePathElement]]]) -> Optional[GetItemResponse]:
        if fields_path_elements is not None:
            kwargs = self._fields_paths_elements_to_expressions(fields_path_elements=fields_path_elements)
        else:
            kwargs = dict()
        kwargs["Key"] = {index_name: key_value}
        kwargs["ConsistentRead"] = True

        try:
            table = self.dynamodb.Table(self.table_name)
            response = table.get_item(**kwargs)
            if "Item" in response:
                processed_item = DynamoDBUtils.dynamodb_to_python_higher_level(dynamodb_object=response['Item'])
                return GetItemResponse(item=processed_item, success=True)
            else:
                return GetItemResponse(item=None, success=False)
        except ResourceNotExistsError:
            raise Exception(f"DynamoDb table {self.table_name} do not exist or in the process of being created. Failed to get attributes from DynamoDb table.")
        except Exception as e:
            print(f"Failed to retrieve attributes from DynamoDb table. Exception of type {type(e).__name__} occurred: {str(e)}")
            return None

    def check_if_item_exist_by_primary_key(self, index_name: str, key_value: str, fields_path_elements: Optional[List[str]]) -> Optional[bool]:
        raise Exception("Not implemented")  # todo: implement

    def _execute_update_query(self, query_kwargs_dict: dict, allow_validation_exception: bool = False) -> Optional[Response]:
        try:
            table = self.dynamodb.Table(self.table_name)
            response = table.update_item(**query_kwargs_dict)
            return Response(response)
        except ResourceNotExistsError:
            raise Exception(f"DynamoDb table {self.table_name} do not exist or in the process of being created. Failed to get attributes from DynamoDb table.")
        except ClientError as e:
            print(f"{e} - No element has been found for the update query : {query_kwargs_dict}")
            if allow_validation_exception and e.response['Error']['Code'] == 'ValidationException':
                return Response({})
            return None
        except Exception as e:
            print(f"Failed to update attributes in DynamoDb table. Exception of type {type(e).__name__} occurred: {str(e)}")
            return None

    def add_data_elements_to_list(self, index_name: str, key_value: Any, object_path: str,
                                  element_values: List[dict]) -> Optional[Response]:
        kwargs = {
            "TableName": self.table_name,
            "Key": {index_name: key_value},
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

    def remove_data_elements_from_list(self, index_name: str, key_value: Any, list_object_path: str, indexes_to_remove: list) -> Optional[Response]:
        kwargs = {
            "TableName": self.table_name,
            "Key": {index_name: key_value},
            "ReturnValues": "UPDATED_NEW"
        }
        update_expression = "REMOVE "
        for i, index_in_database_list in enumerate(indexes_to_remove):
            update_expression += f"{list_object_path}[{index_in_database_list}]"
            if i + 1 < len(indexes_to_remove):
                update_expression += ", "
        kwargs["UpdateExpression"] = update_expression

        return self._execute_update_query(query_kwargs_dict=kwargs)

    def remove_data_elements_from_map(
            self, index_name: str, key_value: Any,
            targets_path_elements: List[List[DatabasePathElement]],
            retrieve_removed_elements: bool = False
    ) -> Optional[Dict[str, Any]]:

        consumed_targets_path_elements: List[List[DatabasePathElement]] = list()
        expression_attribute_names_dict = dict()
        update_expression = "REMOVE "

        for i_target, target in enumerate(targets_path_elements):
            current_target_num_path_elements = len(target)
            current_setter_attribute_names = dict()
            current_remover_update_expression = ""

            for i_path_element, path_element in enumerate(target):
                current_path_key = f"#target{i_target}_pathKey{i_path_element}"
                current_remover_update_expression += current_path_key
                current_setter_attribute_names[current_path_key] = path_element.element_key
                if i_path_element + 1 < current_target_num_path_elements:
                    current_remover_update_expression += "."

            complete_update_expression_bytes_size = getsizeof(update_expression)
            current_setter_update_expression_bytes_size = getsizeof(current_remover_update_expression)
            update_expression_bytes_size_if_setter_is_added = complete_update_expression_bytes_size + current_setter_update_expression_bytes_size
            if update_expression_bytes_size_if_setter_is_added < EXPRESSION_MAX_BYTES_SIZE:
                if i_target > 0:
                    update_expression += ", "
                expression_attribute_names_dict = {**expression_attribute_names_dict, **current_setter_attribute_names}
                update_expression += current_remover_update_expression
                consumed_targets_path_elements.append(target)
            else:
                break

        update_query_kwargs = {
            'TableName': self.table_name,
            'Key': {index_name: key_value},
            'ReturnValues': "UPDATED_OLD" if retrieve_removed_elements is True else "NONE",
            'UpdateExpression': update_expression,
            'ExpressionAttributeNames': expression_attribute_names_dict,
        }
        response = self._execute_update_query(query_kwargs_dict=update_query_kwargs, allow_validation_exception=True)
        if response is None:
            return None

        # Even if we return an empty output_response_attributes dict, we do not want to return None instead of a dict, because since this function only returns a dict, a return
        # value of None indicates that the operation failed. Where as, for example in delete operation, we will not request the removed attributes from the database, which will
        # give us an empty output_response_attributes dict, but the delete operation will base itself on the presence of the dict to judge if the operation failed or not.
        output_response_attributes: Dict[str, Any] = DynamoDBUtils.dynamodb_to_python_higher_level(response.attributes) if response.attributes is not None else {}
        if len(targets_path_elements) == len(consumed_targets_path_elements):
            return output_response_attributes
        else:
            for current_target_path_elements in consumed_targets_path_elements:
                targets_path_elements.remove(current_target_path_elements)

            request_continuation_response_attributes = self.remove_data_elements_from_map(
                index_name=index_name, key_value=key_value,
                targets_path_elements=targets_path_elements,
                retrieve_removed_elements=retrieve_removed_elements
            )
            combined_output_response_attributes = {
                **(output_response_attributes or dict()),
                **(request_continuation_response_attributes or dict())
            }
            return combined_output_response_attributes

    @staticmethod
    def _add_database_path_element_to_string_expression(
            base_string: Optional[str], database_path_element: DatabasePathElement, path_key: str
    ) -> Tuple[str, Dict[str, str]]:

        output_string: str = base_string or str()
        output_expression_attribute_names: Dict[str, str] = dict()

        if isinstance(database_path_element.element_key, str):
            if len(output_string) > 0:
                output_string += "."
            output_string += path_key
            output_expression_attribute_names[path_key] = database_path_element.element_key
        elif isinstance(database_path_element.element_key, int):
            # If the element_key is an int, it means we try to access an index of a list or set. We can right away use the index access
            # quotation ( [$index] instead of .$attributeName ) and not need to pass the index as an attribute name. Note, that anyway,
            # boto3 will only accept strings as attribute names, and trying to pass an int or float as attribute name, will crash the request.
            output_string += f"[{database_path_element.element_key}]"

        return output_string, output_expression_attribute_names

    @staticmethod
    def _add_database_path_element_to_string_path(base_string: Optional[str], database_path_element: DatabasePathElement) -> str:
        output_string: str = base_string or str()
        if isinstance(database_path_element.element_key, str):
            if len(output_string) > 0:
                output_string += "."
            output_string += database_path_element.element_key
        elif isinstance(database_path_element.element_key, int):
            # See comment on similar block code in function _add_database_path_element_to_string_expression
            output_string += f"[{database_path_element.element_key}]"
        return output_string

    @staticmethod
    def _prepare_map_initialization(
            i: int, current_path_element: DatabasePathElement, overriding_init_value: Any,
            previous_element_initializer_container: Optional[MapItemInitializerContainer] = None
    ) -> MapItemInitializer:

        current_path_key = f"#pathKey{i}"
        current_path_target = str() if previous_element_initializer_container is None else previous_element_initializer_container.item.path_target

        current_path_target, new_expression_attribute_names = DynamoDbCoreAdapter._add_database_path_element_to_string_expression(
            base_string=current_path_target, database_path_element=current_path_element, path_key=current_path_key
        )
        expression_attribute_names = (
            {**previous_element_initializer_container.item.expression_attribute_names, **new_expression_attribute_names}
            if previous_element_initializer_container is not None else new_expression_attribute_names
        )

        item_default_value = overriding_init_value or current_path_element.get_default_value()
        return MapItemInitializer(
            path_target=current_path_target, last_item_element_key=current_path_element.element_key,
            item_default_value=item_default_value, expression_attribute_names=expression_attribute_names
        )

    def initialize_element_in_map_target(
            self, index_name: str, key_value: Any, initializer: MapItemInitializer, is_last_path_element: bool = False
    ) -> Optional[Response]:

        base_query_kwargs = {
            'TableName': self.table_name,
            'Key': {index_name: key_value},
            'ExpressionAttributeNames': initializer.expression_attribute_names,
            'ExpressionAttributeValues': {
                ':item': initializer.item_default_value
            },
            'ReturnValues': 'UPDATED_NEW' if is_last_path_element else 'NONE'
            # We do not need to waste retrieving the UPDATED_NEW value when we know we we
            # will it only for the last path element (we use right after the loop ended)
        }
        current_update_expression = f"SET {initializer.path_target} = if_not_exists({initializer.path_target}, :item)"
        current_set_potentially_missing_object_query_kwargs = {**base_query_kwargs, 'UpdateExpression': current_update_expression}
        response = self._execute_update_query(query_kwargs_dict=current_set_potentially_missing_object_query_kwargs)
        if is_last_path_element is True:
            # If the last item attribute value in the database does (retrieved thanks to the "UPDATED_NEW" ReturnValues),
            # do not equal its default_value (which is either the field type default value, or the
            # last_item_custom_overriding_init_value parameter), we want to initialize again the item, because this means
            # that the attribute existed, so the SET update expression with if_not_exists did not performed, and we do
            # not currently have the value we want in the database. We perform this operation only for the last path
            # element in the request, to avoid overriding and scratching list or dictionary that are expected to exist.
            field_existing_attribute_in_database = response.attributes.get(initializer.last_item_element_key) if response is not None else None
            if field_existing_attribute_in_database is None or field_existing_attribute_in_database != initializer.item_default_value:
                # It is possible for the field to already exist, and yet, to not have
                current_update_expression = f"SET {initializer.path_target} = :item"
                current_override_existing_object_value_query_kwargs = {**base_query_kwargs, 'UpdateExpression': current_update_expression}
                override_object_value_response = self._execute_update_query(query_kwargs_dict=current_override_existing_object_value_query_kwargs)
                return override_object_value_response
        return response

    def _construct_update_data_element_to_map_query_kwargs(
            self, index_name: str, key_value: Any, field_path_elements: List[DatabasePathElement], value: Any
    ) -> dict:

        expression_attribute_names_dict = dict()
        update_expression = "SET "

        for i, path_element in enumerate(field_path_elements):
            current_path_key = f"#pathKey{i}"
            update_expression += current_path_key
            expression_attribute_names_dict[current_path_key] = path_element.element_key
            if i + 1 < len(field_path_elements):
                update_expression += "."
            else:
                update_expression += " = :item"

        update_query_kwargs = {
            "TableName": self.table_name,
            "Key": {index_name: key_value},
            "ReturnValues": "UPDATED_NEW",
            "UpdateExpression": update_expression,
            "ExpressionAttributeNames": expression_attribute_names_dict,
            "ExpressionAttributeValues": {
                ":item": value
            }
        }
        return update_query_kwargs

    def set_update_data_element_to_map_with_default_initialization(
            self, index_name: str, key_value: Any, field_path_elements: List[DatabasePathElement], value: Any
    ) -> Optional[Response]:
        update_query_kwargs = self._construct_update_data_element_to_map_query_kwargs(
            index_name=index_name, key_value=key_value, field_path_elements=field_path_elements, value=value
        )
        return self._execute_update_query_with_initialization_if_missing(
            index_name=index_name, key_value=key_value, update_query_kwargs=update_query_kwargs,
            setters=[DynamoDBMapObjectSetter(field_path_elements=field_path_elements, value_to_set=value)]
        )

    def set_update_data_element_to_map_without_default_initialization(
            self, index_name: str, key_value: Any, field_path_elements: List[DatabasePathElement], value: Any
    ) -> Optional[Response]:
        update_query_kwargs = self._construct_update_data_element_to_map_query_kwargs(
            index_name=index_name, key_value=key_value, field_path_elements=field_path_elements, value=value
        )
        return self._execute_update_query(query_kwargs_dict=update_query_kwargs)

    @staticmethod
    def _setters_to_tidied_initializers(setters: List[DynamoDBMapObjectSetter]) -> Dict[str, MapItemInitializerContainer]:
        root_initializers_containers: Dict[str, MapItemInitializerContainer] = dict()
        all_initializers_containers: Dict[str, MapItemInitializerContainer] = dict()

        for setter in setters:
            current_absolute_target_path = str()
            last_map_initializer_container: Optional[MapItemInitializerContainer] = None

            for i_path, path_element in enumerate(setter.field_path_elements):
                current_absolute_target_path = DynamoDbCoreAdapter._add_database_path_element_to_string_path(
                    base_string=current_absolute_target_path, database_path_element=path_element
                )
                existing_container: Optional[MapItemInitializerContainer] = all_initializers_containers.get(current_absolute_target_path, None)
                if existing_container is None:
                    overriding_init_value: Optional[Any] = setter.value_to_set if i_path + 1 >= len(setter.field_path_elements) else None
                    current_map_initializer = DynamoDbCoreAdapter._prepare_map_initialization(
                        i=i_path, current_path_element=path_element, overriding_init_value=overriding_init_value,
                        previous_element_initializer_container=last_map_initializer_container
                    )
                    current_map_initializer_container = MapItemInitializerContainer(item=current_map_initializer, nexts_in_line=dict())
                    all_initializers_containers[current_absolute_target_path] = current_map_initializer_container
                    # We must use the current_absolute_target_path as key, because the all_initializers_containers dict is a flattened dict.

                    # We can use the element_key instead of the absolute path as key for both the root_initializers_containers and last_map_initializer_container
                    # dict's since they are layered dictionnaries, unlike the all_initializers_containers dict which is a flattened dict.
                    if last_map_initializer_container is None:
                        # If the last_map_initializer_container is None, this means that we are in the first iteration of the
                        # field_path_elements loop, which means that the current_map_initializer_container is a root initializer.
                        root_initializers_containers[path_element.element_key] = current_map_initializer_container
                    else:
                        # If the last_map_initializer_container is not None, we know that our current_map_initializer_container is not
                        # a root initializer, and needs to be set in the nexts_in_line variable of the last_map_initializer_container.
                        last_map_initializer_container.nexts_in_line[path_element.element_key] = current_map_initializer_container

                    last_map_initializer_container = current_map_initializer_container
                else:
                    last_map_initializer_container = existing_container
                    logging.info(message_with_vars(
                        message="Successfully tidied duplicate DatabasePathElement initializer",
                        vars_dict={
                            'trimmedPathElementKey': path_element.element_key,
                            'trimmedSetterValueToSet': setter.value_to_set,
                            'existingContainerItemPathTarget': existing_container.item.path_target,
                            'existingContainerItemDefaultValue': existing_container.item.item_default_value
                        }
                    ))
        return root_initializers_containers

    def _initializer_task_executor(self, index_name: str, key_value: str, initializer_container: MapItemInitializerContainer) -> Optional[Response]:
        # todo: group fields initialization in single request
        is_last_path_element = not len(initializer_container.nexts_in_line) > 0
        initialization_response = self.initialize_element_in_map_target(
            index_name=index_name, key_value=key_value,
            initializer=initializer_container.item,
            is_last_path_element=is_last_path_element
        )
        if initialization_response is None:
            logging.error(message_with_vars(
                message="Initialized a field after a set/update multiple data elements in map request had failed.",
                vars_dict={'initializer_container': initializer_container}
            ))

        if len(initializer_container.nexts_in_line) > 0:
            results = self._run_initializers_in_executors(
                index_name=index_name, key_value=key_value,
                initializers=initializer_container.nexts_in_line
            )
        return initialization_response

    def _run_initializers_in_executors(self, index_name: str, key_value: str, initializers: Dict[str, MapItemInitializerContainer]) -> Optional[List[Optional[Response]]]:
        initializers_values = initializers.values()
        num_initializers = len(initializers_values)
        with ThreadPoolExecutor(max_workers=num_initializers) as executor:
            results: List[Optional[Response]] = [executor.submit(
                self._initializer_task_executor, index_name, key_value, item
            ).result() for i, item in enumerate(initializers_values)]
        return results

    def _execute_update_query_with_initialization_if_missing(
            self, index_name: str, key_value: Any, update_query_kwargs: dict, setters: List[DynamoDBMapObjectSetter]
    ) -> Optional[Response]:

        response = self._execute_update_query(query_kwargs_dict=update_query_kwargs)
        if response is None:
            # If the response is None, it means that one of the path of the target path has not been found and need to be initialized.
            database_paths_initializers = self._setters_to_tidied_initializers(setters=setters)
            results = self._run_initializers_in_executors(
                index_name=index_name, key_value=key_value,
                initializers=database_paths_initializers
            )
            last_response = results[-1]
            # This function can only be triggered if there is at least one item in setters List, so we do not need to worry about accessing
            # the last element in result List without checking if the len of the setters or of the results is superior to zero.
            return last_response
        return response

    def set_update_multiple_data_elements_to_map(
            self, index_name: str, key_value: Any, setters: List[DynamoDBMapObjectSetter]
    ) -> Optional[Response]:

        if not len(setters) > 0:
            # If we tried to run the query with no object setter,
            # she will crash when executed. So we return None.
            return None

        update_query_kwargs = {
            "TableName": self.table_name,
            "Key": {index_name: key_value},
            "ReturnValues": "UPDATED_NEW"
        }
        update_expression = "SET "
        expression_attribute_names_dict, expression_attribute_values_dict = dict(), dict()

        consumed_setters: List[DynamoDBMapObjectSetter] = list()
        for i_setter, current_setter in enumerate(setters):
            current_setter_update_expression = ""
            current_setter_attribute_names, current_setter_attribute_values = dict(), dict()
            for i_path, current_path_element in enumerate(current_setter.field_path_elements):
                current_path_key = f"#setter{i_setter}_pathKey{i_path}"
                current_setter_update_expression, new_expression_attribute_names = DynamoDbCoreAdapter._add_database_path_element_to_string_expression(
                    base_string=current_setter_update_expression, database_path_element=current_path_element, path_key=current_path_key
                )
                current_setter_attribute_names = {**current_setter_attribute_names, **new_expression_attribute_names}

                if i_path >= (len(current_setter.field_path_elements) - 1):
                    current_setter_update_expression += f" = :item{i_setter}"
                    current_setter_attribute_values[f":item{i_setter}"] = current_setter.value_to_set

            complete_update_expression_bytes_size = getsizeof(update_expression)
            # complete_update_query_attribute_names_bytes_size = getsizeof(expression_attribute_names_dict)
            # complete_update_query_attribute_values_bytes_size = getsizeof(expression_attribute_values_dict)
            current_setter_update_expression_bytes_size = getsizeof(current_setter_update_expression)
            # current_setter_attribute_names_bytes_size = getsizeof(current_setter_attribute_names)
            # current_setter_attribute_values_bytes_size = getsizeof(current_setter_attribute_values)
            update_expression_bytes_size_if_setter_is_added = complete_update_expression_bytes_size + current_setter_update_expression_bytes_size
            # attributes_names_bytes_size_if_setter_is_added = complete_update_query_attribute_names_bytes_size + current_setter_attribute_names_bytes_size
            # attributes_values_bytes_size_if_setter_is_added = complete_update_query_attribute_values_bytes_size + current_setter_attribute_values_bytes_size
            if update_expression_bytes_size_if_setter_is_added < EXPRESSION_MAX_BYTES_SIZE:
                if i_setter > 0:
                    update_expression += ", "
                update_expression += current_setter_update_expression
                expression_attribute_names_dict = {**expression_attribute_names_dict, **current_setter_attribute_names}
                expression_attribute_values_dict = {**expression_attribute_values_dict, **current_setter_attribute_values}
                consumed_setters.append(current_setter)
            else:
                print(message_with_vars(
                    message="Update operation expression size has reached over 4kb. "
                            "The operation will be divided in a secondary operation (which could also be divided)",
                    vars_dict={
                        'index_name': index_name, 'key_value': key_value,
                        'setters': setters, 'update_expression': update_expression,
                        'current_setter_update_expression': current_setter_update_expression,
                        'current_setter_attribute_names': current_setter_attribute_names,
                        'current_setter_attribute_values': current_setter_attribute_values
                    }
                ))
                break

        update_query_kwargs["UpdateExpression"] = update_expression
        update_query_kwargs["ExpressionAttributeValues"] = expression_attribute_values_dict
        if len(expression_attribute_names_dict) > 0:
            update_query_kwargs["ExpressionAttributeNames"] = expression_attribute_names_dict

        response = self._execute_update_query_with_initialization_if_missing(
            index_name=index_name, key_value=key_value,
            update_query_kwargs=update_query_kwargs, setters=consumed_setters
        )
        if len(consumed_setters) == len(setters):
            return response
        else:
            for setter in consumed_setters:
                setters.remove(setter)
            return self.set_update_multiple_data_elements_to_map(index_name=index_name, key_value=key_value, setters=setters)

    def query_by_key(
            self, index_name: str, key_value: Any,
            fields_path_elements: Optional[List[List[DatabasePathElement]]] = None,
            filter_expression: Optional[Any] = None, query_limit: Optional[int] = None, **additional_kwargs
    ) -> Response:
        if fields_path_elements is not None:
            kwargs = self._fields_paths_elements_to_expressions(fields_path_elements=fields_path_elements)
        else:
            kwargs = dict()
        if additional_kwargs is not None:
            kwargs = {**kwargs, **additional_kwargs}

        kwargs["KeyConditionExpression"] = Key(index_name).eq(key_value)
        if index_name != self.primary_index.hash_key_name:
            # If the queried index is the primary_index, it must
            # not be specified, otherwise the request will fail.
            kwargs["IndexName"] = index_name

        if filter_expression is not None:
            kwargs["FilterExpression"] = filter_expression

        if query_limit is not None:
            kwargs["Limit"] = query_limit

        try:
            table = self.dynamodb.Table(self.table_name)
            response = table.query(**kwargs)
            return Response(DynamoDBUtils.dynamodb_to_python(response))
        except ResourceNotExistsError:
            raise Exception(f"DynamoDb table {self.table_name} do not exist or in the process"
                            "of being created. Failed to get attributes from DynamoDb table.")
        except Exception as e:
            raise Exception(f"Failed to retrieve attributes from DynamoDb table."
                            f"Exception of type {type(e).__name__} occurred: {str(e)}")

    def query_single_item_by_key(
            self, index_name: str, key_value: Any,
            fields_path_elements: Optional[List[List[DatabasePathElement]]] = None,
            filter_expression: Optional[Any] = None
    ) -> Optional[dict]:
        # Yes, a query request is heavier than a get request that we could do with the _get_item_by_primary_key function.
        # Yet, in a get request, we cannot specify an index_name to query on. So, the _query_single_item_by_key should be
        # used when we want to get an item based on another index that the primary one. Otherwise, use _get_item_by_primary_key
        response = self.query_by_key(
            index_name=index_name, key_value=key_value,
            fields_path_elements=fields_path_elements,
            filter_expression=filter_expression, query_limit=1
        )
        if response.count == 1:
            return response.items[0]
        elif not response.count > 0:
            print("No item has been found")
            return None
        elif not response.count > 1:
            print("More than one item has been found. Returning first item.")
            return response.items[0]

    def get_or_query_single_item(self, index_name: str, key_value: str, fields_path_elements: List[List[DatabasePathElement]]) -> Optional[dict]:
        if self.primary_index.hash_key_name == index_name:
            response_item: Optional[dict] = self.get_item_by_primary_key(
                index_name=index_name, key_value=key_value, fields_path_elements=fields_path_elements
            ).item
            return response_item
        else:
            if index_name not in self._global_secondary_indexes_hash_keys:
                print(message_with_vars(
                    message="A index_name was not the primary_index index_name, and was not found in the "
                            "global_secondary_indexes hash_keys. Database query not executed, and None is being returned.",
                    vars_dict={
                        'primary_index.hash_key_name': self.primary_index.hash_key_name,
                        '_global_secondary_indexes_hash_keys': self._global_secondary_indexes_hash_keys,
                        'index_name': index_name, 'key_value': key_value, "fields_path_elements": fields_path_elements
                    }
                ))
                return None
            else:
                response_items: Optional[List[dict]] = self.query_by_key(
                    index_name=index_name, key_value=key_value,
                    fields_path_elements=fields_path_elements, query_limit=1
                ).items
                if isinstance(response_items, list) and len(response_items) > 0:
                    return response_items[0]
                else:
                    return None

    def get_data_in_path_target(
            self, index_name: str, key_value: str,
            field_path_elements: List[DatabasePathElement],
            num_keys_to_navigation_into: int
    ) -> Optional[any]:

        response_item = self.get_or_query_single_item(
            index_name=index_name, key_value=key_value,
            fields_path_elements=[field_path_elements]
        )
        return self.navigate_into_data_with_field_path_elements(
            data=response_item, field_path_elements=field_path_elements,
            num_keys_to_navigation_into=num_keys_to_navigation_into
        )

    def get_value_in_path_target(self, index_name: str, key_value: str, field_path_elements: List[DatabasePathElement]) -> Optional[any]:
        return self.get_data_in_path_target(
            index_name=index_name, key_value=key_value,
            field_path_elements=field_path_elements,
            num_keys_to_navigation_into=len(field_path_elements)
        )

    def get_item_in_path_target(self, index_name: str, key_value: str, field_path_elements: List[DatabasePathElement]) -> Optional[dict]:
        return self.get_data_in_path_target(
            index_name=index_name, key_value=key_value,
            field_path_elements=field_path_elements,
            num_keys_to_navigation_into=len(field_path_elements) - 1
        )

    def get_data_from_multiple_fields_in_path_target(
            self, key_value: str, fields_path_elements: Dict[str, List[DatabasePathElement]],
            num_keys_to_stop_at_before_reaching_end_of_item: int, index_name: Optional[str] = None,
            metadata: bool = False
    ) -> Optional[Dict[str, Any]]:

        response_item: Optional[dict] = self.get_or_query_single_item(
            index_name=index_name, key_value=key_value,
            fields_path_elements=list(fields_path_elements.values())
        )
        if response_item is None:
            return None

        output_dict: Dict[str, Any] = {}
        for path_elements_key, path_elements_item in fields_path_elements.items():
            if len(path_elements_item) > 0:
                num_keys_to_navigation_into: int = len(path_elements_item) - num_keys_to_stop_at_before_reaching_end_of_item
                first_path_element_item_element_key: str = path_elements_item[0].element_key

                current_navigated_response_item_value: Optional[Any] = response_item.get(first_path_element_item_element_key, None)
                if current_navigated_response_item_value is not None:
                    current_navigated_response_item = {first_path_element_item_element_key: current_navigated_response_item_value}
                    # We get separately the in its own dictionary the item with the key of the first_path_element, because when we
                    # get the response_items, they will be all put in the same dict, which means that the response_items dict has
                    # values and items that might have nothing to do with the current target field. Of course, its not an issue,
                    # if we do some navigation into the dicts, but if there is no navigation (for example, if we used the get_item
                    # function and that we queried a base field that does not require any validation), we need to isolate the item
                    # from the other response_items, otherwise, we would return the entire response instead of only the item that
                    # was found in the field that was queried.

                    for i, path_element in enumerate(path_elements_item):
                        if i + 1 > num_keys_to_navigation_into or (not isinstance(current_navigated_response_item, dict)):
                            break

                        current_navigated_response_item = current_navigated_response_item.get(path_element.element_key, None)
                        if current_navigated_response_item is None:
                            # If the current_navigated_response_item is None, it means that one key has not been
                            # found, so we need to break the loop in order to try to call get on a None object,
                            # and then we will return the response_item, so we will return None.
                            break

                    if metadata is not True:
                        output_dict[path_elements_key] = current_navigated_response_item
                    else:
                        output_dict[path_elements_key] = {
                            'value': current_navigated_response_item,
                            'field_path_elements': path_elements_item
                        }
        return output_dict

    def get_values_in_multiple_path_target(
            self, index_name: str, key_value: str,
            fields_path_elements: Dict[str, List[DatabasePathElement]],
            metadata: bool = False
    ):
        return self.get_data_from_multiple_fields_in_path_target(
            index_name=index_name, key_value=key_value,
            fields_path_elements=fields_path_elements,
            num_keys_to_stop_at_before_reaching_end_of_item=0, metadata=metadata
        )

    def get_items_in_multiple_path_target(
            self, index_name: str, key_value: str,
            fields_path_elements: Dict[str, List[DatabasePathElement]],
            metadata: bool = False
    ):
        return self.get_data_from_multiple_fields_in_path_target(
            index_name=index_name, key_value=key_value,
            fields_path_elements=fields_path_elements,
            num_keys_to_stop_at_before_reaching_end_of_item=1, metadata=metadata
        )

    @staticmethod
    def _add_to_filter_expression(expression, condition):
        if expression is None:
            return condition
        return expression & condition

    @staticmethod
    def _fields_paths_elements_to_expressions(fields_path_elements: List[List[DatabasePathElement]]) -> dict:
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
        for i_field, field_elements in enumerate(fields_path_elements):
            for i_path_element, path_element in enumerate(field_elements):
                if i_path_element > 0:
                    last_field_path_element = field_elements[i_path_element - 1]
                    if last_field_path_element.default_type == list:
                        projection_expression += f'[{path_element.element_key}]'
                        # We set the index of a list directly inside the projection expression, because a variable can
                        # be passed the expression_attribute_names only if it is a str, where as our index will be an int.
                        continue
                    elif last_field_path_element.default_type == set:
                        raise Exception("Selection by index in set type's not yet supported")

                # If we reach this point, the continue keyword has not been triggered, which means that the current
                # path_element has not yet been handled by a special case handler, like an list index handler.
                current_field_path_expression_name = f"#f{i_field}_{i_path_element}"
                expression_attribute_names[current_field_path_expression_name] = path_element.element_key
                projection_expression += f'{"." if i_path_element > 0 else ""}{current_field_path_expression_name}'

            if i_field + 1 < len(fields_path_elements):
                projection_expression += ", "

        if len(expression_attribute_names) > 0:
            output_kwargs["ExpressionAttributeNames"] = expression_attribute_names
        if projection_expression.replace(" ", "") != "":
            output_kwargs["ProjectionExpression"] = projection_expression

        return output_kwargs

    @staticmethod
    def navigate_into_data_with_field_path_elements(data: Any, field_path_elements: List[DatabasePathElement], num_keys_to_navigation_into: int) -> Optional[Any]:
        if data is not None:
            num_field_path_elements = len(field_path_elements)
            for i, path_element in enumerate(field_path_elements):
                if i + 1 > num_keys_to_navigation_into:
                    break
                elif i > 0 and field_path_elements[i - 1].default_type == list:
                    data = data[0]
                else:
                    if not isinstance(data, dict):
                        break

                    if path_element.default_type == set and data == {} and num_field_path_elements > i + 1:
                        data = field_path_elements[i + 1].element_key
                        break

                    data = data.get(path_element.element_key, None)
                    if data is None:
                        # If the response_item is None, it means that one key has not been found,
                        # so we need to break the loop in order to try to call get on a None object,
                        # and then we will return the response_item, so we will return None.
                        break
        return data
