from aandeg.handlers import CollectHandler, PostgresHandler
from aandeg.read_json import read_prod_class_data_json
from aandeg.config import config

test_json_str = """
{
  "manifest": "initial load",
  "product_classes": [
    {
      "type": "product_class",
      "pc_id": "hot_burrito_toasted",
      "equip_class_depends": [
        "device_fridge_prep",
        "device_oven",
        "device_toaster"
      ]
    },
    {
      "type": "product_class",
      "pc_id": "cold_fountain_soda",
      "equip_class_depends": [
        "device_fountain_soda",
        "device_ice_maker"
      ]
    }
  ]
}
"""


def test_read_prod_string():
    ch = CollectHandler()
    read_prod_class_data_json(test_json_str, ch, is_filename=False)
    assert(len(ch.prod_class_collect_list) == 2)
    assert(ch.prod_class_collect_list[0].get('type') == 'product_class')
    assert(ch.prod_class_collect_list[0].get('pc_id') == 'hot_burrito_toasted')
    assert(ch.prod_class_collect_list[1].get('pc_id') == 'cold_fountain_soda')
    assert(len(ch.prod_class_collect_list[0].get('depend_ecids')) == 3)
    assert(len(ch.prod_class_collect_list[1].get('depend_ecids')) == 2)

def test_read_prod_file():
    # just make sure we can read the file without errors
    ch = CollectHandler()
    read_prod_class_data_json("./data/product_class.json", ch, is_filename=True)

def test_read_prod_db_handler():
    with PostgresHandler(*config().get_args(), is_testing=True) as pgm:
        read_prod_class_data_json(test_json_str, pgm, is_filename=False)
        cursor = pgm.connection.cursor()
        cursor.execute("""SELECT * FROM prod_class""")
        assert(len(cursor.fetchall()) == 2)
        cursor.execute("""SELECT * FROM prod_class_depends""")
        assert(len(cursor.fetchall()) == 5)

