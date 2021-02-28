from aandeg.handlers import CollectHandler, PostgresHandler
from aandeg.read_json import read_store_data_json
from aandeg.config import config

store_test_json_str = """
{
  "manifest": "initial load",
  "stores": [
    {
      "type": "store",
      "sc_id": "store_default",
      "s_id": "store_1"
    },
    {
      "type": "store",
      "sc_id": "store_default",
      "s_id": "store_2"
    },
    {
      "type": "store",
      "sc_id": "store_default",
      "s_id": "store_3"
    }
  ]
}
"""


def test_read_store_str():
    ch = CollectHandler()
    read_store_data_json(store_test_json_str, ch, is_filename=False)
    assert(len(ch.store_collect_list) == 3)
    assert(ch.store_collect_list[0].get('type') == 'store')
    assert(ch.store_collect_list[0].get('sc_id') == 'store_default')
    assert(ch.store_collect_list[0].get('s_id') == 'store_1')
    assert(ch.store_collect_list[1].get('type') == 'store')
    assert(ch.store_collect_list[1].get('sc_id') == 'store_default')
    assert(ch.store_collect_list[1].get('s_id') == 'store_2')
    assert(ch.store_collect_list[2].get('s_id') == 'store_3')

def test_read_store_file():
    # just make sure we can read the file without errors
    ch = CollectHandler()
    read_store_data_json("./data/store.json", ch, is_filename=True)

def test_read_store_db_handler():
    with PostgresHandler(*config().get_args(), is_testing=True) as pgm:
        read_store_data_json(store_test_json_str, pgm, is_filename=False)
        cursor = pgm.connection.cursor()
        cursor.execute("""SELECT * FROM store""")
        assert(len(cursor.fetchall()) == 3)

