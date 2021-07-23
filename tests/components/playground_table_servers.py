from StructNoSQL import PrimaryIndex
from StructNoSQL.servers_middlewares.external_dynamodb_api.api_executor import ExternalDynamoDBApiExecutor


class InoftVocalEngineExternalDynamoDBApiExecutor(ExternalDynamoDBApiExecutor):
    def __init__(self):
        super().__init__(
            table_name="inoft-vocal-engine_projects-virtual-tables", region_name="eu-west-3",
            primary_index=PrimaryIndex(hash_key_name='accountProjectTableKeyId', hash_key_variable_python_type=str),
            auto_create_table=False
        )
