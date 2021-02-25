import json
from decimal import Decimal
from aandeg.exceptions import MissingDataError, DuplicateEidError, UnknownDependsError


def read_equip_data_json(fn_or_json, handler, is_filename=False, check_depends=False):
    equip_classes_data = None
    if is_filename:
        with open(fn_or_json) as json_file:
            equip_classes_data = json.load(json_file, parse_float=Decimal)
    else:
        equip_classes_data = json.loads(fn_or_json)
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

