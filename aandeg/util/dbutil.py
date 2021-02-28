import psycopg2
from psycopg2 import OperationalError


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def create_tables(connection):
    cursor = connection.cursor()
    cursor.execute(open('data/schema_create.sql', 'r').read())
    cursor.execute("SHOW search_path")
    sp = cursor.fetchone()[0]
    connection.commit()

def drop_tables(connection):
    cursor = connection.cursor()
    cursor.execute(open('./data/schema_drop.sql', 'r').read())
    connection.commit()

# drops all tables in the schema
def drop_all_tables(connection):
    cursor = connection.cursor()
    cursor.execute(
        "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
    rows = cursor.fetchall()
    for row in rows:
        cursor.execute("DROP TABLE " + row[1] + " CASCADE")
    connection.commit()

