import json
from decimal import Decimal
from aandeg.exceptions import MissingDataError, DuplicateIdError, UnknownDependsError
from aandeg.handlers import BaseHandler

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


def read_equip_class_data_json(fn_or_json, handler, is_filename=False, check_depends=False):
    assert(isinstance(handler, BaseHandler))
    equip_class_data = get_json_data(fn_or_json, is_filename)
    for attr in ["manifest", "equip-classes"]:
        if not equip_class_data.get(attr):
            raise MissingDataError("equip-classes missing '{}' attribute".format(attr))
    seen_depends = set()
    seen_ec_id = set()
    for equip_class in equip_class_data.get("equip-classes"):
        for attr in ["type", "ec_id"]:
            if not equip_class.get(attr):
                raise MissingDataError("equip-class missing '{}' attribute".format(attr))
        ec_id = equip_class.get('ec_id')
        type = equip_class.get('type')
        if ec_id in seen_ec_id:
            raise DuplicateIdError("duplicate ec_id: '{}".format(ec_id))
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
    for attr in ["manifest", "product-classes"]:
        if not prod_class_data.get(attr):
            raise MissingDataError("product-classes missing '{}' attribute".format(attr))
    # check for local duplicates
    seen_pc_id = set()
    for prod_class in prod_class_data.get("product-classes"):
        for attr in ["type", "pc_id"]:
            if not prod_class.get(attr):
                raise MissingDataError("product-class missing '{}' attribute".format(attr))
        pc_id = prod_class.get('pc_id')
        type = prod_class.get('type')
        if pc_id in seen_pc_id:
            raise DuplicateIdError("duplicate pc_id: '{}".format(pc_id))
        depends = []
        if prod_class.get('equip_class_depends'):
            for depend in prod_class.get('equip_class_depends'):
                depends.append(depend)
        if handler:
            handler.handle_prod_class(type, pc_id, depends)

def read_store_class_data_json(fn_or_json, handler, is_filename=False):
    assert(isinstance(handler, BaseHandler))
    store_class_data = get_json_data(fn_or_json, is_filename)
    for attr in ["manifest", "store-classes"]:
        if not store_class_data.get(attr):
            raise MissingDataError("store-classes missing '{}' attribute".format(attr))
    seen_sc_id = set()
    for store_class in store_class_data.get("store-classes"):
        for attr in ["type", "sc_id"]:
            if not store_class.get(attr):
                raise MissingDataError("store-class missing '{}' attribute".format(attr))
        sc_id = store_class.get('sc_id')
        type = store_class.get('type')
        if sc_id in seen_sc_id:
            raise DuplicateIdError("duplicate sc_id: '{}".format(sc_id))
        if handler:
            handler.handle_store_class(type, sc_id)

def read_store_data_json(fn_or_json, handler, is_filename=False):
    assert(isinstance(handler, BaseHandler))
    store_data = get_json_data(fn_or_json, is_filename)
    for attr in ["manifest", "stores"]:
        if not store_data.get(attr):
            raise MissingDataError("stores missing '{}' attribute".format(attr))
    seen_s_id = set()
    for store in store_data.get("stores"):
        for attr in ["type", "s_id", "sc_id"]:
            if not store.get(attr):
                raise MissingDataError("store missing '{}' attribute".format(attr))
        type = store.get('type')
        s_id = store.get('s_id')
        sc_id = store.get('sc_id')
        if s_id in seen_s_id:
            raise DuplicateIdError("duplicate s_id: '{}".format(sc_id))
        if handler:
            handler.handle_store(type, s_id, sc_id)

