from aandeg.handlers import CollectHandler, PostgresHandler
from aandeg.read_json import read_store_data_json
from aandeg.aandeg_util import make_timestamp

store_test_json_str = """
{
  "manifest": "initial load",
  "stores": [
    {
      "type": "store",
      "sc_id": "store-default",
      "s_id": "store-1"
    },
    {
      "type": "store",
      "sc_id": "store-default",
      "s_id": "store-2"
    },
    {
      "type": "store",
      "sc_id": "store-default",
      "s_id": "store-3"
    }
  ]
}
"""


def test_read_store_str():
    ch = CollectHandler()
    read_store_data_json(store_test_json_str, ch, is_filename=False)
    assert(len(ch.store_collect_list) == 3)
    assert(ch.store_collect_list[0].get('type') == 'store')
    assert(ch.store_collect_list[0].get('sc_id') == 'store-default')
    assert(ch.store_collect_list[0].get('s_id') == 'store-1')
    assert(ch.store_collect_list[1].get('type') == 'store')
    assert(ch.store_collect_list[1].get('sc_id') == 'store-default')
    assert(ch.store_collect_list[1].get('s_id') == 'store-2')
    assert(ch.store_collect_list[2].get('s_id') == 'store-3')

def test_read_store_file():
    # just make sure we can read the file without errors
    filename = "../data/store.json"
    ch = CollectHandler()
    read_store_data_json(filename, ch, is_filename=True)

def test_read_store_db_handler():
    temp = '_' + make_timestamp()
    with PostgresHandler("aandeg", "jdann", "", "localhost", 5432, table_suffix=temp) as pgm:
        read_store_data_json(store_test_json_str, pgm, is_filename=False)
        cursor = pgm.connection.cursor()
        cursor.execute("""SELECT * FROM {}""".format(pgm.store_table_name))
        assert(len(cursor.fetchall()) == 3)

