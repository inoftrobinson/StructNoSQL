from StructNoSQL import DynamoDBTableConnectors, PrimaryIndex


class DynamoDBLowLevelTableClient(DynamoDBTableConnectors):
    def __init__(self):
        self.__setup_connectors__(
            table_name="inoft-vocal-engine_projects-users-data-virtual-tables", region_name="eu-west-3",
            primary_index=PrimaryIndex(hash_key_name='accountProjectUserId', hash_key_variable_python_type=str),
            auto_create_table=False
        )
