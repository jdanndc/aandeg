import boto3
from aandeg import dynavail
from botocore.exceptions import ClientError


def test_dynavail_bootstrap():
    client = boto3.client('dynamodb')
    dynavail.bootstrap(client)
    print(client.describe_table(TableName=dynavail.TN_STORE))
    print(client.list_tables())

def test_put():
    dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    table = dynamodb.Table(dynavail.TN_STORE)
    try:
        response = table.put_item(Item={"id": "store-1"})
        response = table.get_item(Key={"id": "store-1"})
        assert(response['Item'] is not None)
        print(response['Item'])
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        assert(True)
    try:
        response = table.delete_item(Key={"id":"store-2"})
        store_2_item = {
            "id": "store-2",
            "location": { "City": "Media", "State": "PA" },
            "products": {
                "meatball_sub": {"available": True},
                "turkey_hoagie": {"available": True}
            }
        }
        response = table.put_item(Item=store_2_item)
        response = table.get_item(Key={"id": "store-2"})
        assert(response['Item'] is not None)
        assert(response['Item'].get('products') is not None)
        assert(response['Item'].get('products').get('meatball_sub') is not None)
        assert(response['Item'].get('products').get('meatball_sub').get('available') is not None)
        assert(response['Item'].get('products').get('meatball_sub').get('available') == True)
        print(response['Item'])
        # see here for expression info:
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
        table.update_item(
            Key={"id": "store-2"},
            UpdateExpression="SET products.meatball_sub.available=:a",
            ExpressionAttributeValues={
                ':a': False
            },
            ReturnValues="UPDATED_NEW"
        )
        response = table.get_item(Key={"id": "store-2"})
        print(response['Item'])
        assert(response['Item'].get('products') is not None)
        assert(response['Item'].get('products').get('meatball_sub') is not None)
        assert(response['Item'].get('products').get('meatball_sub').get('available') is not None)
        assert(response['Item'].get('products').get('meatball_sub').get('available') == False)
        response = table.put_item(Item=store_2_item)
        response = table.get_item(Key={"id": "store-2"})
        assert(response['Item'].get('products').get('meatball_sub').get('available') == True)
        i = 0
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        assert(True)
