from aandeg.util.config import Config
from aandeg.administer import Administer
from optparse import OptionParser
import json

def print_json(s, indent=2):
    print(json.dumps(s, indent=indent))

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('-i', '--incident', action='store_true', dest="is_incident")
    parser.add_option('-c', '--clear', dest="incident_clear_id")
    parser.add_option('-C', '--clear-all', action='store_true', dest="incident_clear_all")
    parser.add_option('-s', '--store', dest="store")
    parser.add_option('-e', '--equip', dest="equip")
    parser.add_option('-p', '--products', action='store_true', dest="show_store_products")
    parser.add_option('-E', '--list-equip', action='store_true', dest="list_equip")
    parser.add_option('-S', '--list-stores', action='store_true', dest="list_stores")
    parser.add_option('-P', '--list-prod', action='store_true', dest="list_prod")
    parser.add_option('-D', '--list-prod-depend', action='store_true', dest="list_prod_depend")
    parser.add_option('-Q', '--list-equip-depend', action='store_true', dest="list_equip_depend")
    parser.add_option('-I', '--list-incidents', action='store_true', dest="list_incidents")
    parser.add_option('-a', '--availability-mode', action='store_true', dest="is_availability_mode")
    parser.add_option('-u', '--update', action='store_true', dest="is_update")
    parser.add_option('-U', '--update-all', action='store_true', dest="is_update_all")
    parser.add_option('-v', '--verbose', action='store_true', dest="is_verbose")
    (opt, args) = parser.parse_args()

    admin = Administer(Config().create_connection())
    if not opt.is_availability_mode:
        if opt.list_equip:
            print_json(admin.list_equip())
        elif opt.list_stores:
            print_json(admin.list_stores())
        elif opt.list_prod:
            print_json(admin.list_products())
        elif opt.list_equip_depend:
            print_json(admin.list_table('equip_class_depends'))
        elif opt.list_prod_depend:
            print_json(admin.list_table('prod_class_depends'))
        elif opt.list_incidents:
            if opt.incident_clear_all:
                admin.model.clear_all_incidents()
            print_json(admin.list_table('incident_report'))
        elif opt.store:
            if opt.is_incident:
                if opt.equip:
                    admin.create_incident_report(opt.store, opt.equip)
                elif opt.incident_clear_id is not None:
                    admin.clear_incident(opt.incident_clear_id)
                elif opt.incident_clear_all:
                    admin.clear_store_incidents(opt.store)
            print_json(admin.get_store(opt.store, opt.is_incident, opt.show_store_products))
        elif opt.is_incident:
            print(*admin.list_table('incident_report'), sep='\n')
    else:
        if opt.store:
            if opt.is_update:
                print_json(admin.avail_update_store(opt.store))
            else:
                print_json(admin.avail_list_store(opt.store))
        elif opt.list_stores:
            print_json(admin.avail_list_stores(opt.is_verbose))
        elif opt.is_update_all:
            print_json(admin.avail_update_all_stores())
        else:
            pass
        pass
    pass


