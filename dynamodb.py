import os
import json
import boto3

from decimal import Decimal
from typing import Dict, List, Any, Union
from boto3.dynamodb.conditions import Key


_aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "")
_aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")

_users_table_name = "brit_users"
_users_items_table_name = "brit_users_items"

_dynamodb = boto3.resource('dynamodb', aws_access_key_id=_aws_access_key_id, aws_secret_access_key=_aws_secret_access_key, region_name="eu-west-1")


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def user_get(username: str) -> Union[Dict[str, str], None]:
    users_table = _dynamodb.Table(_users_table_name)
    res = users_table.query(KeyConditionExpression=Key("username").eq(username))
    users = res.get("Items", [])
    if users:
        return users[0]

    return None


def user_put(username: str, password: str):
    users_table = _dynamodb.Table(_users_table_name)
    user = dict(username=username, password=password)
    users_table.put_item(Item=user)


def user_del(username: str):
    users_table = _dynamodb.Table(_users_table_name)
    user_key = dict(username=username)
    users_table.delete_item(Key=user_key)


def user_items_get_all(username: str) -> List[Dict[str, Any]]:
    users_items_table = _dynamodb.Table(_users_items_table_name)
    res = users_items_table.query(KeyConditionExpression=Key("username").eq(username))
    user_items_records = res.get("Items", [])
    user_items_records = list(map(lambda x: json.loads(json.dumps(x, cls=DecimalEncoder)), user_items_records))
    return user_items_records


def user_items_put(username: str, timestamp: int, items: List[Dict[str, Any]]):
    users_items_table = _dynamodb.Table(_users_items_table_name)
    user_items = dict()
    user_items["username"] = username
    user_items["timestamp"] = timestamp
    user_items["items"] = items
    user_items = json.loads(json.dumps(user_items), parse_float=Decimal)
    users_items_table.put_item(Item=user_items)


def user_items_del(username: str, timestamp: int):
    users_items_table = _dynamodb.Table(_users_items_table_name)
    user_items_key = dict(username=username, timestamp=timestamp)
    users_items_table.delete_item(Key=user_items_key)


def user_items_del_all(username: str):
    user_items_records = user_items_get_all(username)
    for user_items_rec in user_items_records:
        user_items_del(username, user_items_rec["timestamp"])
