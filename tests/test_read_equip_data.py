from aandeg.data_handler.postgres import PostgresHandler
from aandeg.data_handler.collect import CollectHandler
from aandeg.util.read_json import read_equip_class_data_json
from aandeg.util.config import Config
from aandeg.administer import Administer


def test_read_equip_data():
    str = """
    {
        "manifest": "initial load",
        "equip_classes": [
            {
                "type": "equip_class",
                "ec_id": "water_supply"
            },
            {
                "type": "equip_class",
                "ec_id": "water_main",
                "depends": [
                    "water_supply"
                ]
            }
        ]
    }
    """
    ch = CollectHandler()
    read_equip_class_data_json(str, ch)
    assert(len(ch.equip_class_collect_list) == 2)
    conn = Config().connection()
    with PostgresHandler(conn, is_testing=True) as pgm:
        read_equip_class_data_json("./data/equip_class.json", pgm, is_filename=True)
        cursor = pgm.connection.cursor()
        cursor.execute("select count(*) from equip_class")
        assert(cursor.fetchone()[0] == 24)
        admin = Administer(conn)
        admin.update_imputed_depends()
        pass

