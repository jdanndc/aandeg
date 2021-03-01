from aandeg.util.config import Config
from aandeg.model import Model
from aandeg.data_handler.postgres import PostgresHandler
from aandeg.util.read_json import read_equip_class_data_json, read_prod_class_data_json, read_store_class_data_json, read_store_data_json


# testing helper method
# is_testing=True, so all actions within the scope of the context manager write to the same unique temp schema
def prepare_for_incident_test(pgh: PostgresHandler, model: Model):
    read_equip_class_data_json("./data/equip_class.json", pgh, is_filename=True)
    read_prod_class_data_json("./data/product_class.json", pgh, is_filename=True)
    read_store_class_data_json("./data/store_class.json", pgh, is_filename=True)
    read_store_data_json("./data/store.json", pgh, is_filename=True)
    model.update_imputed_depends()
    model.update_store_classes_with_all_products()


def test_incident():
    conn = Config().create_connection()
    with PostgresHandler(conn, is_testing=True) as pgh:
        model = Model(conn)
        prepare_for_incident_test(pgh, model)
        s_id = 'store_1'
        model.clear_store_incidents(s_id)
        ec_id = 'device_water_filtration'
        incident_id = model.create_incident_report(s_id, ec_id)
        print("created incident with id: {}".format(incident_id))

        incidents = model.get_store_incidents(s_id)
        dd = [ { "equip_class": x[2], "status": x[3] }  for x in incidents]
        print("---------------\nunavailable:\n")
        for sp in model.get_unavailable_store_products(s_id):
            print(sp)

        print("---------------\navailable:\n")
        for sp in model.get_available_store_products(s_id):
            print(sp)


def test_store_is_open():
    conn = Config().create_connection()
    with PostgresHandler(conn, is_testing=True) as pgh:
        model = Model(conn)
        prepare_for_incident_test(pgh, model)
        s_id = 'store_1'
        model.clear_store_incidents(s_id)
        assert(model.store_is_open(s_id))
        assert(len(model.get_available_store_products(s_id)) > 0)
        incident_id = model.create_incident_report(s_id, 'device_hvac')
        assert(not model.store_is_open(s_id))
        assert(len(model.get_available_store_products(s_id)) == 0)

