import inspect
from aandeg.model import Model
from aandeg.data_handler.base import INCIDENT_REPORT_TYPE_FAIL
from aandeg.availability import Availability

class Administer(object):
    def __init__(self, db_conn):
        self.connection = db_conn
        self.model = Model(self.connection)
        self.availability = Availability()

    def create_incident_report(self, s_id, ec_id, itype=INCIDENT_REPORT_TYPE_FAIL, description=""):
        return self.ok_status(
            {
                "incident_id": self.model.create_incident_report(s_id, ec_id, itype, description)
            }
        )

    def clear_store_incidents(self, s_id):
        pre_count = len(self.model.get_store_incidents(s_id))
        self.model.clear_store_incidents(s_id)
        post_count = len(self.model.get_store_incidents(s_id))
        return self.ok_status(
            {
                "store": self._get_store_info(s_id),
                "pre_count": pre_count,
                "post_count": post_count
            }
        )

    def clear_incident(self, i_id):
        pre_count = len(self.model.get_incidents())
        self.model.clear_incident(i_id)
        post_count = len(self.model.get_incidents())
        return self.ok_status(
            {
                "incident_id": i_id,
                "pre_count": pre_count,
                "post_count": post_count
            }
        )

    def get_store(self, s_id, include_incidents=False, include_products=False):
        store_info = self._get_store_info(s_id)
        if include_incidents:
            store_incidents = self.model.get_store_incidents(s_id)
            store_info['incidents'] = [{"id": x[0], "equip_class": x[2], "status": x[3]} for x in store_incidents]
        if include_products:
            store_info['products'] = {}
            store_info['products']['available'] = [ x[0] for x in self.model.get_available_store_products(s_id) ]
            store_info['products']['unavailable'] = [ x[0] for x in self.model.get_unavailable_store_products(s_id) ]
        payload = {
            "store": store_info
        }
        return self.ok_status(payload)

    # TODO: beware big tables! :)
    def list_table(self, table_name):
        table_listing = self.model.list_table(table_name)
        payload = {
            "table_name": table_name,
            "table_list": [
                table_listing
            ]
        }
        return self.ok_status(payload)

    def list_equip(self):
        return self.ok_status( { "equip_class": [ x[0] for x in self.model.list_table('equip_class') ] } )

    def list_stores(self):
        return self.ok_status( { "store": [ { "id": x[0], "type": x[1] } for x in self.model.list_table('store') ] } )

    def list_products(self):
        return self.ok_status( { "prod_class": [ x[0] for x in self.model.list_table('prod_class') ] } )

    def _get_store_info(self, s_id):
        return {
            "store_id": s_id,
            "is_open": self.model.store_is_open(s_id)
        }

    def ok_status(self, payload):
        return {
            "result": {
                "status": "OK",
                "method": inspect.stack()[1][3],
            },
            "payload" : payload
        }

    # beware, we are scanning all
    # TODO: use FilterExpressions
    def avail_list_stores(self, is_verbose=False):
        self.availability.get_table()
        scan_kwargs = {
            #'FilterExpression': 'is_open = :is_open',
            #'ExpressionAttributeValues : {
            #   ':is_open': true
            #}
            #'ExpressionAttributeNames': {"#is_open": "UNK"}
        }
        if not is_verbose:
            scan_kwargs['ProjectionExpression'] = "id, is_open"
        stores = []
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = self.availability.get_table().scan(**scan_kwargs)
            stores.append(response.get('Items'))
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
        return stores

    def avail_list_store(self, store_id):
        response = self.availability.get_table().get_item(Key={"id": store_id})
        return response.get("Item")

    def avail_update_store(self, store_id):
        self.availability.update_store(self.model, store_id)
        return self.avail_list_store(store_id)

    # update table in Availability data store with data from the model data store
    def avail_update_all_stores(self):
        pre_count = self.availability.get_table().item_count
        self.availability.update_all_stores(self.model)
        post_count = self.availability.get_table().item_count
        return self.ok_status(
            {
                "method": "update_all_stores",
                "pre_count": pre_count,
                "post_count": post_count
            }
        )

