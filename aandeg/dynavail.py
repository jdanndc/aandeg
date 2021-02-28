
TN_STORE = 'Store'

def bootstrap(client):
    try:
        # TODO: change from client to this:
        # dynamodb.create_table()
        # dynamodb.Table(name)
        # table.delete()
        print("bootstrap")
        response = client.delete_table(TableName=TN_STORE)
        print("DO -- delete table {}".format(TN_STORE))
        waiter = client.get_waiter('table_not_exists')
        waiter.wait(TableName=TN_STORE, WaiterConfig={'Delay': 3, 'MaxAttempts':20})
        print("OK -- deleted table {}".format(TN_STORE))
    except client.exceptions.ResourceNotFoundException:
        print("warn -- delete table {} -- resource not found".format(TN_STORE))
    try:
        response = client.create_table(
            TableName=TN_STORE,
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
    )
    except client.exceptions.ResourceInUseException:
        print("warn -- delete table {} -- resource not found".format(TN_STORE))
    print(response)

