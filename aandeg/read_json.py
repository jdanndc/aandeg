import json
from decimal import Decimal
from aandeg.exceptions import MissingDataError, DuplicateIdError, UnknownDependsError
from handler.base import BaseHandler


# TODO: these methods could be be tightened up
# read json from either a string or a file, depending on is_filename flag
def get_json_data(fn_or_json, is_filename):
    json_data = None
    if is_filename:
        with open(fn_or_json) as json_file:
            json_data = json.load(json_file, parse_float=Decimal)
    else:
        json_data = json.loads(fn_or_json)
    return json_data


def check_keys(dict_to_check, keys, name="dict"):
    for attr in keys:
        if not dict_to_check.get(attr):
            raise MissingDataError("{} missing '{}' attribute".format(name, attr))


def check_duplicate(key, seen_set, name="key"):
    if key in seen_set:
        raise DuplicateIdError("duplicate {}: '{}".format(name, key))


def read_equip_class_data_json(fn_or_json, handler, is_filename=False, check_depends=False):
    assert(isinstance(handler, BaseHandler))
    equip_class_data = get_json_data(fn_or_json, is_filename)
    check_keys(equip_class_data, ["manifest", "equip_classes"], "equip_classes")
    seen_depends = set()
    seen_ec_id = set()
    for equip_class in equip_class_data.get("equip_classes"):
        check_keys(equip_class, ["type", "ec_id"], "equip_class")
        ec_id = equip_class.get('ec_id')
        type = equip_class.get('type')
        check_duplicate(ec_id, seen_ec_id, "ec_id")
        depends = []
        if equip_class.get('depends'):
            for depend in equip_class.get('depends'):
                if check_depends and (depend not in seen_depends):
                    raise UnknownDependsError("undefined depend '{}' for eclass '{}'".format(depend, ec_id))
                depends.append(depend)
        if handler:
            handler.handle_equip_class(type, ec_id, depends)
        seen_depends.add(ec_id)


def read_prod_class_data_json(fn_or_json, handler, is_filename=False, check_depends=False):
    assert(isinstance(handler, BaseHandler))
    prod_class_data = get_json_data(fn_or_json, is_filename)
    check_keys(prod_class_data, ["manifest", "product_classes"], "product_classes")
    # check for local duplicates
    seen_pc_id = set()
    for prod_class in prod_class_data.get("product_classes"):
        check_keys(prod_class, ["type", "pc_id"], "product_class")
        pc_id = prod_class.get('pc_id')
        type = prod_class.get('type')
        check_duplicate(pc_id, seen_pc_id, "pc_id")
        depends = []
        if prod_class.get('equip_class_depends'):
            for depend in prod_class.get('equip_class_depends'):
                depends.append(depend)
        if handler:
            handler.handle_prod_class(type, pc_id, depends)


def read_store_class_data_json(fn_or_json, handler, is_filename=False):
    assert(isinstance(handler, BaseHandler))
    store_class_data = get_json_data(fn_or_json, is_filename)
    check_keys(store_class_data, ["manifest", "store_classes"], "store_classes")
    seen_sc_id = set()
    for store_class in store_class_data.get("store_classes"):
        check_keys(store_class, ["type", "sc_id"], "store_class")
        sc_id = store_class.get('sc_id')
        type = store_class.get('type')
        check_duplicate(sc_id, seen_sc_id, "sc_id")
        if handler:
            handler.handle_store_class(type, sc_id)


def read_store_data_json(fn_or_json, handler, is_filename=False):
    assert(isinstance(handler, BaseHandler))
    store_data = get_json_data(fn_or_json, is_filename)
    check_keys(store_data, ["manifest", "stores"], "stores")
    seen_s_id = set()
    for store in store_data.get("stores"):
        check_keys(store, ["type", "s_id", "sc_id"], "store")
        type = store.get('type')
        s_id = store.get('s_id')
        sc_id = store.get('sc_id')
        check_duplicate(s_id, seen_s_id, "s_id")
        if handler:
            handler.handle_store(type, s_id, sc_id)

