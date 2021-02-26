from aandeg.handlers import CollectHandler, PostgresHandler
from aandeg.read_json import read_equip_class_data_json
from aandeg.aandeg_util import make_timestamp

def test_read_equip_data():
    str = """
    {
        "manifest": "initial load",
        "equip-classes": [
            {
                "type": "equip-class",
                "ec_id": "water-supply"
            },
            {
                "type": "equip-class",
                "ec_id": "water-main",
                "depends": [
                    "water-supply"
                ]
            }
        ]
    }
    """
    ch = CollectHandler()
    read_equip_class_data_json(str, ch)
    assert(len(ch.equip_class_collect_list) == 2)
    temp = '_' + make_timestamp()
    filename = "../data/equip_class.json"
    with PostgresHandler("aandeg", "jdann", "", "localhost", 5432, table_suffix=temp) as pgm:
        read_equip_class_data_json(filename, pgm, is_filename=True)
        pgm.update_imputed_depends()

