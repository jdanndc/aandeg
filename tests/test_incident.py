from aandeg.util.config import Config
from aandeg.administer import Administer
from aandeg.data_handler.postgres import PostgresHandler
from aandeg.util.read_json import read_equip_class_data_json, read_prod_class_data_json, read_store_class_data_json, read_store_data_json


# testing helper method
# is_testing=True, so all actions within the scope of the context manager write to the same unique temp schema
def prepare_for_incident_test(pgh: PostgresHandler, admin: Administer):
    read_equip_class_data_json("./data/equip_class.json", pgh, is_filename=True)
    read_prod_class_data_json("./data/product_class.json", pgh, is_filename=True)
    read_store_class_data_json("./data/store_class.json", pgh, is_filename=True)
    read_store_data_json("./data/store.json", pgh, is_filename=True)
    admin.update_imputed_depends()
    admin.update_store_classes_with_all_products()


def test_incident():
    conn = Config().connection()
    with PostgresHandler(conn, is_testing=True) as pgh:
        admin = Administer(conn)
        prepare_for_incident_test(pgh, admin)
        s_id = 'store_1'
        admin.clear_store_incidents(s_id)
        ec_id = 'device_water_filtration'
        incident_id = admin.create_incident_report(s_id, ec_id)
        print("created incident with id: {}".format(incident_id))

        print("---------------\nunavailable:\n")
        for sp in admin.get_unavailable_store_products(s_id):
            print(sp)

        print("---------------\navailable:\n")
        for sp in admin.get_available_store_products(s_id):
            print(sp)


def test_store_is_open():
    conn = Config().connection()
    with PostgresHandler(conn, is_testing=True) as pgh:
        admin = Administer(conn)
        prepare_for_incident_test(pgh, admin)
        s_id = 'store_1'
        admin.clear_store_incidents(s_id)
        assert(admin.store_is_open(s_id))
        assert(len(admin.get_available_store_products(s_id)) > 0)
        incident_id = admin.create_incident_report(s_id, 'device_hvac')
        assert(not admin.store_is_open(s_id))
        assert(len(admin.get_available_store_products(s_id)) == 0)

