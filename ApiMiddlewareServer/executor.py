from typing import Optional, Any

from pydantic import ValidationError

from ApiMiddlewareServer.database_client import DynamoDBLowLevelTableClient
from ApiMiddlewareServer.parser import StructNoSQLParser



class StructNoSQLExecutor:
    def __init__(self):
        self.table = DynamoDBLowLevelTableClient()

    def execute(self, data: dict):
        handler: Optional[Any] = StructNoSQLParser.parse(data=data)
        if handler is None:
            return {'success': False, 'errorKey': 'operationNotSupported'}

        try:
            success, data, kwargs = handler(self.table, data)
            return {'success': success, 'data': data, **(kwargs or {})}
        except ValidationError as e:
            return {'success': False, 'errorKey': 'validationError', 'exception': str(e)}
