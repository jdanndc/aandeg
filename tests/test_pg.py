from util import create_connection

def test_connect():
    db_name = 'aandeg'
    db_user = 'jdann'
    db_password = ''
    db_host = 'localhost'
    db_port = 5432
    conn = create_connection(db_name, db_user, db_password, db_host, db_port)
    assert(conn)



