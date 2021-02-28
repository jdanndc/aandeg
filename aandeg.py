from aandeg.util.config import Config
from aandeg.administer import Administer
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

    admin = Administer(Config().connection())
    if opt.list_equip:
        print(*admin.list_table('equip_class'), sep='\n')
    elif opt.list_stores:
        print(*admin.list_table('store'), sep='\n')
    elif opt.list_prod:
        print(*admin.list_table('prod_class'), sep='\n')
    elif opt.list_equip_depend:
        print(*admin.list_table('equip_class_depends'), sep='\n')
    elif opt.list_prod_depend:
        print(*admin.list_table('prod_class_depends'), sep='\n')
    elif opt.store:
        if opt.is_incident:
            if opt.equip:
                admin.create_incident_report(opt.store, opt.equip)
            elif opt.incident_clear_id is not None:
                admin.clear_incident(opt.incident_clear_id)
            elif opt.incident_clear_all:
                admin.clear_store_incidents(opt.store)
            print(*admin.get_store_incidents(opt.store), sep='\n')
        elif opt.show_store_open:
            print("open={}".format(admin.store_is_open(opt.store)))
        elif opt.show_available:
            print(*admin.get_available_store_products(opt.store), sep='\n')
        elif opt.show_unavailable:
            print(*admin.get_unavailable_store_products(opt.store), sep='\n')
    elif opt.is_incident:
        print(*admin.list_table('incident_report'), sep='\n')

    pass


