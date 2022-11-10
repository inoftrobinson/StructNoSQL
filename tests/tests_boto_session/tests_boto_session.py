import unittest
import uuid
from typing import Optional

import boto3
from pydantic import BaseModel, ValidationError

from tests.components.playground_table_clients import PlaygroundDynamoDBBasicTable, TEST_ACCOUNT_ID, TEST_PROJECT_ID, \
    ENGINE_API_KEY, PROD_ACCOUNT_ID, PROD_PROJECT_ID
from StructNoSQL import TableDataModel, BaseField, FieldGetter


class DynamoDBTableModel(TableDataModel):
    accountProjectTableKeyId = BaseField(field_type=str, required=True)
    simpleField = BaseField(field_type=str, required=False)


class TestsBotoSession(unittest.TestCase):
    def __init__(self, method_name: str):
        super().__init__(methodName=method_name)

    def test_boto_session(self):
        field_random_value: str = f"randomFieldValue_{uuid.uuid4()}"

        session = boto3.Session()
        table_client = PlaygroundDynamoDBBasicTable(data_model=DynamoDBTableModel, boto_session=session)
        update_success: bool = table_client.update_field(
            key_value=TEST_ACCOUNT_ID, field_path='simpleField', value_to_set=field_random_value
        )
        self.assertTrue(update_success)

        retrieved_field_value: Optional[str] = table_client.get_field(
            key_value=TEST_ACCOUNT_ID, field_path='simpleField'
        )
        self.assertEqual(retrieved_field_value, field_random_value)

    def _make_engine_authorized_boto_session(self):
        import requests
        authorization_response: requests.Response = requests.get(
            f"http://127.0.0.1:5000/api/v1/{PROD_ACCOUNT_ID}/{PROD_PROJECT_ID}/structnosql-authorization?accessToken={ENGINE_API_KEY}&"
        )
        self.assertTrue(authorization_response.ok)

        authorization_response_payload = authorization_response.json()
        class AuthorizationResponseDataModel(BaseModel):
            AccessKeyId: str
            SecretAccessKey: str
            SessionToken: str
        try:
            authorization_response_data = AuthorizationResponseDataModel(**authorization_response_payload)
        except ValidationError as e:
            self.fail(msg=e)

        authorized_session = boto3.Session(
            aws_access_key_id=authorization_response_data.AccessKeyId,
            aws_secret_access_key=authorization_response_data.SecretAccessKey,
            aws_session_token=authorization_response_data.SessionToken,
        )
        return authorized_session

    def test_engine_credentials_boto_session(self):
        field_random_value: str = f"randomFieldValue_{uuid.uuid4()}"

        authorized_session = self._make_engine_authorized_boto_session()

        from tests.components.prod_inoft_vocal_engine_table_clients import ProdInoftVocalEngineTableBasicClient
        table_client = ProdInoftVocalEngineTableBasicClient(
            data_model=DynamoDBTableModel,
            boto_session=authorized_session,
            auto_create_table=False
            # We specify auto_create_table to False because
            # the inoft-vocal-engine authorization does
            # not allow to describe or create table.
        )

        valid_key_value = f"{PROD_ACCOUNT_ID}-{PROD_PROJECT_ID}-table1-exampleRecordKey"
        valid_update_success: bool = table_client.update_field(
            key_value=valid_key_value, field_path='simpleField', value_to_set=field_random_value
        )
        self.assertTrue(valid_update_success)

        retrieved_field_value: Optional[str] = table_client.get_field(
            key_value=valid_key_value, field_path='simpleField'
        )
        self.assertEqual(retrieved_field_value, field_random_value)

        invalid_key_value = f"invalidAccountId-invalidProjectId-table1-exampleRecordKey"
        invalid_update_success: bool = table_client.update_field(
            key_value=invalid_key_value, field_path='simpleField', value_to_set=field_random_value
        )
        self.assertFalse(invalid_update_success)

    def test_boto_session_with_auto_leading_key(self):
        field_random_value: str = f"randomFieldValue_{uuid.uuid4()}"
        auto_leading_key: str = f"{PROD_ACCOUNT_ID}-{PROD_PROJECT_ID}-table1-"

        authorized_session = self._make_engine_authorized_boto_session()

        from tests.components.prod_inoft_vocal_engine_table_clients import ProdInoftVocalEngineTableBasicClient
        table_client = ProdInoftVocalEngineTableBasicClient(
            data_model=DynamoDBTableModel,
            boto_session=authorized_session,
            auto_create_table=False,
            # We specify auto_create_table to False because
            # the inoft-vocal-engine authorization does
            # not allow to describe or create table.
            auto_leading_key=auto_leading_key
        )

        valid_update_success: bool = table_client.update_field(
            key_value="exampleRecordKey", field_path='simpleField', value_to_set=field_random_value
        )
        self.assertTrue(valid_update_success)

        retrieved_field_value_with_auto_leading_key: Optional[str] = table_client.get_field(
            key_value="exampleRecordKey", field_path="accountProjectTableKeyId"
        )
        self.assertEqual(retrieved_field_value_with_auto_leading_key, "exampleRecordKey")

        table_client.auto_leading_key = None  # We forcefully disable the auto_leading_key
        retrieved_field_value_with_auto_leading_key_disabled: Optional[str] = table_client.get_field(
            key_value="exampleRecordKey", field_path="accountProjectTableKeyId"
        )
        self.assertIsNone(retrieved_field_value_with_auto_leading_key_disabled)

        retrieved_field_value_with_auto_leading_key_disabled_and_manual_key_value: Optional[str] = table_client.get_field(
            key_value=f"{auto_leading_key}exampleRecordKey", field_path="accountProjectTableKeyId"
        )
        self.assertEqual(retrieved_field_value_with_auto_leading_key_disabled_and_manual_key_value, f"{auto_leading_key}exampleRecordKey")
