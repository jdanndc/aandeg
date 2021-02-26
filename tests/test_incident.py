from aandeg.util import aandeg_config
from aandeg.handlers import PostgresHandler
from read_json import read_equip_class_data_json, read_prod_class_data_json, read_store_class_data_json, read_store_data_json


def test_incident():
    with PostgresHandler(*aandeg_config().get_args(), is_testing=True) as pgm:
        read_equip_class_data_json("./data/equip_class.json", pgm, is_filename=True)
        pgm.update_imputed_depends()
        s_id = 'store-1'
        pgm.clear_store_incidents(s_id)
        ec_id = 'device-water-filtration'
        incident_id = pgm.create_incident_report(s_id, ec_id)
        print("created incident with id: {}".format(incident_id))

        print("---------------\nunavailable:\n")
        for sp in pgm.get_unavailable_store_products(s_id):
            print(sp)

        print("---------------\navailable:\n")
        for sp in pgm.get_available_store_products(s_id):
            print(sp)

def test_store_is_open():
    with PostgresHandler(*aandeg_config().get_args(), is_testing=True) as pgm:
        read_equip_class_data_json("./data/equip_class.json", pgm, is_filename=True)
        pgm.update_imputed_depends()
        read_prod_class_data_json("./data/product_class.json", pgm, is_filename=True)
        read_store_class_data_json("./data/store_class.json", pgm, is_filename=True)
        read_store_data_json("./data/store.json", pgm, is_filename=True)
        pgm.update_store_classes_with_all_products()

        s_id = 'store-1'
        pgm.clear_store_incidents(s_id)
        assert(pgm.store_is_open(s_id))
        assert(len(pgm.get_available_store_products(s_id)) > 0)
        incident_id = pgm.create_incident_report(s_id, 'device-hvac')
        assert(not pgm.store_is_open(s_id))
        assert(len(pgm.get_available_store_products(s_id)) == 0)
