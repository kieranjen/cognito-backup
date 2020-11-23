import json
import os
from datetime import datetime, date

import boto3


def get_groups(cognito_client):
    response = cognito_client.list_groups(
        UserPoolId=os.getenv('USER_POOL_ID')
    )

    groups = [item.get("GroupName") for item in response.get("Groups", [])]
    next_token = response.get("NextToken")

    while next_token:
        response = cognito_client.list_groups(
            UserPoolId=os.getenv('USER_POOL_ID'),
            NextToken=next_token
        )

        groups = groups + [item.get("GroupName") for item in response.get("Groups", [])]
        next_token = response.get("NextToken")

    return groups


def get_users(cognito_client):
    response = cognito_client.list_users(
        UserPoolId=os.getenv('USER_POOL_ID')
    )

    users = response.get("Users", [])
    next_token = response.get("PaginationToken")

    while next_token:
        response = cognito_client.list_users(
            UserPoolId=os.getenv('USER_POOL_ID'),
            PaginationToken=next_token
        )

        users = users + response.get("Users", [])
        next_token = response.get("PaginationToken")

    return users


def get_users_in_group(cognito_client, group_name):
    response = cognito_client.list_users_in_group(
        UserPoolId=os.getenv('USER_POOL_ID'),
        GroupName=group_name
    )

    users = {user.get("Username"): "Exists" for user in response.get("Users", [])}
    next_token = response.get("NextToken")

    while next_token:
        response = cognito_client.list_users(
            UserPoolId=os.getenv('USER_POOL_ID'),
            PaginationToken=next_token
        )

        users = {**users, **{user.get("Username"): "Exists" for user in response.get("Users", [])}}
        next_token = response.get("NextToken")

    return users


def add_groups_to_user(groups, user, users_in_group):
    user_groups = [group for group in groups if users_in_group.get(group, {}).get(user.get("Username"))]
    user["Groups"] = user_groups
    return user


def write_backup_to_file(users_dict):
    json_users = json.dumps({"Users": users_dict}, indent=4, default=serialize_date)
    with open("cognito_backup.json", "w") as backup_file:
        backup_file.write(json_users)


def serialize_date(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def upload_to_s3():
    s3_client = boto3.client("s3")
    s3_client.upload_file("cognito_backup.json", os.getenv("BACKUP_BUCKET"), "backups/cognito_backup.json")


def lambda_handler(event, context):
    cognito_client = boto3.client('cognito-idp')
    groups = get_groups(cognito_client)
    users = get_users(cognito_client)
    users_in_group = {group: get_users_in_group(cognito_client, group) for group in groups}
    users_with_groups = [add_groups_to_user(groups, user, users_in_group) for user in users]
    write_backup_to_file(users_with_groups)
    upload_to_s3()


if __name__ == '__main__':
    lambda_handler(None, None)
