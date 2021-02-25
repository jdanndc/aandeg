from aandeg.handlers import PrintHandler, CollectHandler, PostgresHandler
from aandeg.read_json import read_equip_data_json, read_prod_data_json
from aandeg.aandeg_util import make_timestamp

test_json_str = """
{
  "manifest": "initial load",
  "product-classes": [
    {
      "ptype": "product-class",
      "pid": "hot-burrito-toasted",
      "equip_depends": [
        "device-fridge-prep",
        "device-oven",
        "device-toaster"
      ]
    },
    {
      "ptype": "product-class",
      "pid": "cold-fountain-soda",
      "equip_depends": [
        "device-fountain-soda",
        "device-ice-maker"
      ]
    }
  ]
}
"""


def test_read_prod_string():
    ch = CollectHandler()
    read_prod_data_json(test_json_str, ch, is_filename=False)
    assert(len(ch.prod_collect_list) == 2)
    assert(ch.prod_collect_list[0].get('ptype') == 'product-class')
    assert(ch.prod_collect_list[0].get('pid') == 'hot-burrito-toasted')
    assert(ch.prod_collect_list[1].get('pid') == 'cold-fountain-soda')
    assert(len(ch.prod_collect_list[0].get('depend_eids')) == 3)
    assert(len(ch.prod_collect_list[1].get('depend_eids')) == 2)

def test_read_prod_file():
    # just make sure we can read the file without errors
    filename = "../data/product_class.json"
    ph = PrintHandler()
    read_prod_data_json(filename, ph, is_filename=True)

def test_read_prod_db_handler():
    temp = '_' + make_timestamp()
    with PostgresHandler("aandeg", "jdann", "", "localhost", 5432, table_suffix=temp) as pgm:
        read_prod_data_json(test_json_str, pgm, is_filename=False)
        cursor = pgm.connection.cursor()
        cursor.execute("""SELECT * FROM {}""".format(pgm.prod_class_table_name))
        assert(len(cursor.fetchall()) == 2)
        cursor.execute("""SELECT * FROM {}""".format(pgm.prod_depends_table_name))
        assert(len(cursor.fetchall()) == 5)

