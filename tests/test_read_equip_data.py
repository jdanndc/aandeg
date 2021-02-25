from aandeg.handlers import PrintHandler, CollectHandler, PostgresHandler
from aandeg.read_json import read_equip_data_json
from aandeg.pgutil import make_timestamp

def test_read_equip_data():
    filename = "../data/equip_class.json"
    ph = PrintHandler()
    read_equip_data_json(filename, ph, is_filename=True)
    str = """
    {
        "manifest": "initial load",
        "equip-classes": [
            {
                "etype": "equip-class",
                "eid": "water-supply"
            },
            {
                "etype": "equip-class",
                "eid": "water-main",
                "depends": [
                    "water-supply"
                ]
            }
        ]
    }
    """
    read_equip_data_json(str, ph)
    ch = CollectHandler()
    read_equip_data_json(str, ch)
    assert(len(ch.collect_list)==2)
    temp = '_' + make_timestamp()
    with PostgresHandler("aandeg", "jdann", "", "localhost", 5432, table_suffix=temp) as pgm:
        read_equip_data_json(filename, pgm, is_filename=True)
        pgm.update_imputed_depends()

