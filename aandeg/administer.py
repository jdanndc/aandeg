from aandeg.data_handler.base import EC_DEPEND_TYPE_IMPUTED, INCIDENT_REPORT_TYPE_FAIL,  STORE_OPEN_EQUIP_ID
from aandeg.model import Model
import inspect

class Administer(object):
    def __init__(self, db_conn):
        self.connection = db_conn
        self.model = Model(self.connection)
        pass

    #@staticmethod
    # TODO: change to staticmethod
    def ok_status(self, payload):
        return {
            "result": {
                "status": "OK",
                "method": inspect.stack()[1][3],
            },
            "payload" : payload
        }

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

    def _get_store_info(self, s_id):
        return {
            "store_id": s_id,
            "is_open": self.model.store_is_open(s_id)
        }


