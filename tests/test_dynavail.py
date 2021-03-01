from botocore.exceptions import ClientError
from aandeg.availability import Availability
from aandeg.model import Model
from aandeg.util import read_json
from aandeg.data_handler.postgres import PostgresHandler
from aandeg.util.config import Config


def test_dynavail_bootstrap():
    avail = Availability()
    avail.drop_table()
    avail.create_table()
    avail.dynamodb.meta.client.describe_table(TableName=avail.table_name)
    print(avail.dynamodb.meta.client.list_tables())


def test_put():
    with Availability(is_testing=True) as avail:
        table = avail.dynamodb.Table(avail.table_name)
        try:
            response = table.put_item(Item={"id": "store_1"})
            response = table.get_item(Key={"id": "store_1"})
            assert(response['Item'] is not None)
            print(response['Item'])
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            assert(True)

def test_put_update():
    with Availability(is_testing=True) as avail:
        table = avail.dynamodb.Table(avail.table_name)
        try:
            response = table.delete_item(Key={"id":"store-2"})
            store_2_item = {
                "id": "store_2",
                "location": { "City": "Media", "State": "PA" },
                "products": {
                    "meatball_sub": {"available": True},
                    "turkey_hoagie": {"available": True}
                }
            }
            response = table.put_item(Item=store_2_item)
            response = table.get_item(Key={"id": "store_2"})
            assert(response['Item'] is not None)
            assert(response['Item'].get('products') is not None)
            assert(response['Item'].get('products').get('meatball_sub') is not None)
            assert(response['Item'].get('products').get('meatball_sub').get('available') is not None)
            assert(response['Item'].get('products').get('meatball_sub').get('available') == True)
            print(response['Item'])
            # see here for expression info:
            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html
            table.update_item(
                Key={"id": "store_2"},
                UpdateExpression="SET products.meatball_sub.available=:a",
                ExpressionAttributeValues={
                    ':a': False
                },
                ReturnValues="UPDATED_NEW"
            )
            response = table.get_item(Key={"id": "store_2"})
            print(response['Item'])
            assert(response['Item'].get('products') is not None)
            assert(response['Item'].get('products').get('meatball_sub') is not None)
            assert(response['Item'].get('products').get('meatball_sub').get('available') is not None)
            assert(response['Item'].get('products').get('meatball_sub').get('available') == False)
            response = table.put_item(Item=store_2_item)
            response = table.get_item(Key={"id": "store_2"})
            assert(response['Item'].get('products').get('meatball_sub').get('available') == True)
            i = 0
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            assert(True)


def test_delete_temp_tables():
    Availability().delete_temp_tables()
    assert(True)



def test_put_availability_from_model():
    # ALERT:
    # everything here will be done in the test postgresql schema, and a temp dynamodb table.
    # (due to the context managers, 'with')
    # use the debugger and breakpoints to watch it happen
    conn = Config().create_connection()
    with Availability(is_testing=True) as availability, PostgresHandler(conn, is_testing=True) as pgh:
        model = Model(conn)
        read_json.read_equip_class_data_json("./data/equip_class.json", pgh, is_filename=True)
        read_json.read_prod_class_data_json("./data/product_class.json", pgh, is_filename=True)
        read_json.read_store_class_data_json("./data/store_class.json", pgh, is_filename=True)
        read_json.read_store_data_json("./data/store.json", pgh, is_filename=True)
        model.update_imputed_depends()
        model.update_store_classes_with_all_products()

        for store_id in [ x[0] for x in model.list_table("store") ]:
            availability.update_store(model, store_id)

        table = availability.get_table()
        assert(table.item_count == len(model.list_table("store")))

        response = table.get_item(Key={"id": "store_2"})
        pass

