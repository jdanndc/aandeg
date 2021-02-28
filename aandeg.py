from aandeg.config import config
from handler.postgres import PostgresHandler
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
    parser.add_option('-E', '--list-equip', action='store_true', dest="list_equip")
    parser.add_option('-S', '--list-stores', action='store_true', dest="list_stores")
    parser.add_option('-P', '--list-prod', action='store_true', dest="list_prod")
    parser.add_option('-D', '--list-prod-depend', action='store_true', dest="list_prod_depend")
    parser.add_option('-Q', '--list-equip-depend', action='store_true', dest="list_equip_depend")
    (opt, args) = parser.parse_args()

    with PostgresHandler(*config().get_args()) as pgh:
        if opt.list_equip:
            print(*pgh.list_table('equip_class'), sep='\n')
        elif opt.list_stores:
            print(*pgh.list_table('store_class'), sep='\n')
        elif opt.list_prod:
            print(*pgh.list_table('prod_class'), sep='\n')
        elif opt.list_equip_depend:
            print(*pgh.list_table('equip_class_depends'), sep='\n')
        elif opt.list_prod_depend:
            print(*pgh.list_table('prod_class_depends'), sep='\n')
        elif opt.store:
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
        elif opt.is_incident:
            print(*pgh.list_table('incident_report'), sep='\n')

    pass


