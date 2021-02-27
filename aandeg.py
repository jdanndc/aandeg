from aandeg.config import config
from aandeg.handlers import PostgresHandler
from optparse import OptionParser


if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-i', '--incident', action='store_true', dest="is_incident")
    parser.add_option('-c', '--clear', dest="incident_clear_id")
    parser.add_option('-C', '--clear-all', action='store_true', dest="incident_clear_all")
    parser.add_option('-s', '--store', dest="store")
    parser.add_option('-e', '--equip', dest="equip")
    parser.add_option('-a', '--available', action='store_true', dest="show_available")
    parser.add_option('-u', '--unavailable', action='store_true', dest="show_unavailable")
    parser.add_option('-o', '--open', action='store_true', dest="show_store_open")
    (opt, args) = parser.parse_args()

    with PostgresHandler(*config().get_args()) as pgh:
        if opt.store:
            if opt.is_incident:
                if opt.equip:
                    pgh.create_incident_report(opt.store, opt.equip)
                elif opt.incident_clear_id is not None:
                    pgh.clear_incident(opt.incident_clear_id)
                elif opt.incident_clear_all:
                    pgh.clear_store_incidents(opt.store)
                print(*pgh.get_store_incidents(opt.store), sep='\n')
            elif opt.show_store_open:
                print("open={}".format(pgh.store_is_open(opt.store)))
            elif opt.show_available:
                print(*pgh.get_available_store_products(opt.store), sep='\n')
            elif opt.show_unavailable:
                print(*pgh.get_unavailable_store_products(opt.store), sep='\n')

    pass


