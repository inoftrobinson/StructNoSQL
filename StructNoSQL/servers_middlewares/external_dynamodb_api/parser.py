from typing import Any, Optional

from StructNoSQL.servers_middlewares.external_dynamodb_api.api_executor_handlers import getSingleValueInPathTarget, \
    getValuesInMultiplePathTarget, setUpdateMultipleDataElementsToMap, setUpdateMultipleDataElementsToMapWithDefaultInitialization,\
    removeDataElementsFromMap, deleteDataElementsFromMap, getOrQuerySingleItem, deleteRecord, removeRecord, putRecord, queryItemsByKey


class StructNoSQLParser:
    HANDLERS_SWITCH = {
        'putRecord': putRecord,
        'deleteRecord': deleteRecord,
        'removeRecord': removeRecord,
        'getOrQuerySingleItem': getOrQuerySingleItem,
        'getSingleValueInPathTarget': getSingleValueInPathTarget,
        'getValuesInMultiplePathTarget': getValuesInMultiplePathTarget,
        'setUpdateMultipleDataElementsToMap': setUpdateMultipleDataElementsToMap,
        'setUpdateMultipleDataElementsToMapWithDefaultInitialization': setUpdateMultipleDataElementsToMapWithDefaultInitialization,
        'removeDataElementsFromMap': removeDataElementsFromMap,
        'deleteDataElementsFromMap': deleteDataElementsFromMap,
        'queryItemsByKey': queryItemsByKey
    }

    @staticmethod
    def parse(data: dict) -> Optional[Any]:
        operation_type: Optional[str] = data.get('operationType', None)
        if operation_type is None or not isinstance(operation_type, str):
            return None

        handler = StructNoSQLParser.HANDLERS_SWITCH.get(operation_type, None)
        if handler is None:
            print(f"No handler found for {operation_type}")
            return None

        return handler
