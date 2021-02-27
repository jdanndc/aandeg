import psycopg2
from os import path
from psycopg2 import OperationalError


def file_to_json_data(filename):
    data = None
    if filename:
        if path.exists(filename):
            with open(filename, 'r') as file:
                data = file.read()
        else:
            raise Exception("file not found {}".format(filename))
    return data


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
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


