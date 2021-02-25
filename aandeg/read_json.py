import json
from decimal import Decimal
from aandeg.exceptions import MissingDataError, DuplicateEidError, UnknownDependsError


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
    seen_eid = set()
    for equip_class in equip_classes_data.get("equip-classes"):
        for attr in ["etype", "eid"]:
            if not equip_class.get(attr):
                raise MissingDataError("equip-class missing '{}' attribute".format(attr))
        eid = equip_class.get('eid')
        etype = equip_class.get('etype')
        if eid in seen_eid:
            raise DuplicateEidError("duplicate eid: '{}".format(eid))
        depends = []
        if equip_class.get('depends'):
            for depend in equip_class.get('depends'):
                if check_depends and (depend not in seen_depends):
                    raise UnknownDependsError("undefined depend '{}' for eclass '{}'".format(depend, eid))
                depends.append(depend)
        if handler:
            handler.handle_equip(etype, eid, depends)
        seen_depends.add(eid)


def read_prod_data_json(fn_or_json, handler, is_filename=False, check_depends=False):
    prod_classes_data = get_json_data(fn_or_json, is_filename)
    for attr in ["manifest", "product-classes"]:
        if not prod_classes_data.get(attr):
            raise MissingDataError("product-classes missing '{}' attribute".format(attr))
    # check for local duplicates
    seen_pid = set()
    for prod_class in prod_classes_data.get("product-classes"):
        for attr in ["ptype", "pid"]:
            if not prod_class.get(attr):
                raise MissingDataError("product-class missing '{}' attribute".format(attr))
        pid = prod_class.get('pid')
        ptype = prod_class.get('ptype')
        if pid in seen_pid:
            raise DuplicateEidError("duplicate pid: '{}".format(pid))
        depends = []
        if prod_class.get('equip_depends'):
            for depend in prod_class.get('equip_depends'):
                depends.append(depend)
        if handler:
            handler.handle_prod(ptype, pid, depends)

