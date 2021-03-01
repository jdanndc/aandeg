import boto3
import traceback
import time
from aandeg.model import Model

TN_STORE = 'Store'

class Availability(object):
    def __init__(self, **kwargs):
        self.table_name = TN_STORE
        self.is_testing = False
        if kwargs.get("table_name"):
            self.table_name = kwargs.get("table_name")
        if kwargs.get("is_testing"):
            self.is_testing = kwargs.get("is_testing")
            if not kwargs.get("table_name"):
                self.table_name = "{}_{}".format(TN_STORE, round(time.time()*1000))
        self.dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
        pass

    def __enter__(self):
        if self.is_testing:
            self.create_table()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)
            # return False in order to let exception pass through
            return False
        if self.is_testing:
            self.drop_table()
        return True

    def drop_table(self):
        try:
            response = self.dynamodb.meta.client.delete_table(TableName=self.table_name)
            waiter = self.dynamodb.meta.client.get_waiter('table_not_exists')
            waiter.wait(TableName=self.table_name, WaiterConfig={'Delay': 3, 'MaxAttempts': 20})
        except self.dynamodb.meta.client.exceptions.ResourceNotFoundException as e:
            # ok, table does not exist
            pass

    # exiting from the debugger can leave behind temp tables from testing
    def delete_temp_tables(self):
        try:
            for table_name in [x for x in self.dynamodb.meta.client.list_tables()['TableNames'] if
                               x.startswith('Store_')]:
                response = self.dynamodb.meta.client.delete_table(TableName=table_name)
                waiter = self.dynamodb.meta.client.get_waiter('table_not_exists')
                waiter.wait(TableName=table_name, WaiterConfig={'Delay': 3, 'MaxAttempts': 20})
        except:
            # this too will pass
            pass

    def create_table(self):
        try:
            response = self.dynamodb.create_table(
                TableName=self.table_name,
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
            waiter = self.dynamodb.meta.client.get_waiter('table_exists')
            waiter.wait(TableName=self.table_name, WaiterConfig={'Delay': 3, 'MaxAttempts': 20})
            print(response)
        except self.dynamodb.meta.client.exceptions.ResourceInUseException as e:
            print(e)
            raise e


    def get_table(self):
        return  self.dynamodb.Table(self.table_name)

    def update_store(self, model: Model, store_id):
        products = {
            **dict((key, {'available': True}) for key in [x[0] for x in model.get_available_store_products(store_id)]),
            **dict((key, {'available': False}) for key in [x[0] for x in model.get_unavailable_store_products(store_id)])
        }
        store_data = {
            "id": store_id,
            "is_open": model.store_is_open(store_id),
            "products": products
        }
        response = self.get_table().put_item(Item=store_data)
        return response

    def update_all_stores(self, model: Model):
        for store_id in [ x[0] for x in model.list_table('store')]:
            self.update_store(model, store_id)

