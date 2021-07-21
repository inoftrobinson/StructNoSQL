from json import JSONDecodeError
from typing import Optional, Any

from flask import Flask
from flask import request, jsonify
from tests.components.playground_table_servers import InoftVocalEngineExternalDynamoDBApiExecutor


app = Flask(__name__)


def get_access_token_from_args_or_payload() -> Optional[str]:
    arg_access_token: Optional[str] = request.args.get('accessToken', None)
    if arg_access_token is not None:
        return arg_access_token

    try:
        request_payload: Optional[dict] = request.get_json()
        if request_payload is not None and isinstance(request_payload, dict):
            payload_access_token: Optional[str] = request_payload.get('accessToken', None)
            if payload_access_token is not None and isinstance(payload_access_token, str):
                return payload_access_token
    except JSONDecodeError as e:
        pass

    return None


@app.route("/api/v1/<account_id>/<project_id>/database-client", methods=['POST'])
def database_client(account_id: str, project_id: str):
    access_token_id: Optional[str] = get_access_token_from_args_or_payload()
    if access_token_id is None:
        return jsonify(success=False, errorKey='invalidOrMissingAccessToken')

    if access_token_id != 'daisybelle':
        return jsonify(success=False, errorKey='invalidAccessToken')

    try:
        request_payload: Any = request.get_json()
        if not isinstance(request_payload, dict):
            return jsonify(success=False, errorKey='expectedDictPayload')

        return InoftVocalEngineExternalDynamoDBApiExecutor().execute(request.get_json())

    except JSONDecodeError as e:
        return jsonify(success=False, errorKey='payloadInvalidJSONFormat', error=str(e))


if __name__ == '__main__':
    app.run()
