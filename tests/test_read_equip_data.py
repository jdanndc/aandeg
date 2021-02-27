from aandeg.handlers import CollectHandler, PostgresHandler
from aandeg.read_json import read_equip_class_data_json
from aandeg.config import config

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
    with PostgresHandler(*config().get_args(), is_testing=True) as pgm:
        read_equip_class_data_json("./data/equip_class.json", pgm, is_filename=True)
        cursor = pgm.connection.cursor()
        cursor.execute("select count(*) from equip_class")
        assert(cursor.fetchone()[0] == 24)
        pgm.update_imputed_depends()
        pass

