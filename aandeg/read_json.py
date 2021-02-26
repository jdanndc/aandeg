import json
from decimal import Decimal
from aandeg.exceptions import MissingDataError, DuplicateEcIdError, UnknownDependsError


def get_json_data(fn_or_json, is_filename):
    json_data = None
    if is_filename:
        with open(fn_or_json) as json_file:
            json_data = json.load(json_file, parse_float=Decimal)
    else:
        json_data = json.loads(fn_or_json)
    return json_data


def read_equip_data_json(fn_or_json, handler, is_filename=False, check_depends=False):
    equip_classes_data = get_json_data(fn_or_json, is_filename)
    for attr in ["manifest", "equip-classes"]:
        if not equip_classes_data.get(attr):
            raise MissingDataError("equip-classes missing '{}' attribute".format(attr))
    seen_depends = set()
    seen_ec_id = set()
    for equip_class in equip_classes_data.get("equip-classes"):
        for attr in ["type", "ec_id"]:
            if not equip_class.get(attr):
                raise MissingDataError("equip-class missing '{}' attribute".format(attr))
        ec_id = equip_class.get('ec_id')
        type = equip_class.get('type')
        if ec_id in seen_ec_id:
            raise DuplicateEcIdError("duplicate ec_id: '{}".format(ec_id))
        depends = []
        if equip_class.get('depends'):
            for depend in equip_class.get('depends'):
                if check_depends and (depend not in seen_depends):
                    raise UnknownDependsError("undefined depend '{}' for eclass '{}'".format(depend, ec_id))
                depends.append(depend)
        if handler:
            handler.handle_equip_class(type, ec_id, depends)
        seen_depends.add(ec_id)


def read_prod_data_json(fn_or_json, handler, is_filename=False, check_depends=False):
    prod_classes_data = get_json_data(fn_or_json, is_filename)
    for attr in ["manifest", "product-classes"]:
        if not prod_classes_data.get(attr):
            raise MissingDataError("product-classes missing '{}' attribute".format(attr))
    # check for local duplicates
    seen_pc_id = set()
    for prod_class in prod_classes_data.get("product-classes"):
        for attr in ["type", "pc_id"]:
            if not prod_class.get(attr):
                raise MissingDataError("product-class missing '{}' attribute".format(attr))
        pc_id = prod_class.get('pc_id')
        type = prod_class.get('type')
        if pc_id in seen_pc_id:
            raise DuplicateEcIdError("duplicate pc_id: '{}".format(pc_id))
        depends = []
        if prod_class.get('equip_class_depends'):
            for depend in prod_class.get('equip_class_depends'):
                depends.append(depend)
        if handler:
            handler.handle_prod_class(type, pc_id, depends)

