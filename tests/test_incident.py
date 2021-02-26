from aandeg.util import load_config
from aandeg.handlers import PostgresHandler

def test_incident():
    dbinfo = load_config().get('dbinfo')
    with PostgresHandler(dbinfo.get("db_name"), dbinfo.get("db_user"), dbinfo.get("db_pass"), dbinfo.get("db_host"), dbinfo.get("db_port")) as pgm:
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
    dbinfo = load_config().get('dbinfo')
    with PostgresHandler(dbinfo.get("db_name"), dbinfo.get("db_user"), dbinfo.get("db_pass"), dbinfo.get("db_host"), dbinfo.get("db_port")) as pgm:
        s_id = 'store-1'
        pgm.clear_store_incidents(s_id)
        assert(pgm.store_is_open(s_id))
        assert(len(pgm.get_available_store_products(s_id)) > 0)
        incident_id = pgm.create_incident_report(s_id, 'device-hvac')
        assert(not pgm.store_is_open(s_id))
        assert(len(pgm.get_available_store_products(s_id)) == 0)

