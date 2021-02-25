import psycopg2
import json
from os import path
from psycopg2 import OperationalError
from datetime import datetime


def make_timestamp():
    now = datetime.now()
    return now.strftime("%Y_%m_%d_%H_%M_%S")


def load_config():
    config = None
    with open('.aandeg.json') as json_file:
        config = json.load(json_file)
    return config


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


def scratch_cte_query():
    # Common Table Expressions
    q = """
    WITH RECURSIVE parents AS (
	    SELECT
		    eid,
		    eid_parent
	    FROM
	    	equip_depends_2021_02_24_21_53_42
	    WHERE
	    	eid = 'device-ice-maker'
    	UNION
	    	SELECT
	    		e.eid,
    			e.eid_parent
    		FROM
			equip_depends_2021_02_24_21_53_42 e
    		INNER JOIN parents p ON p.eid_parent = e.eid
    ) SELECT
	    *
    FROM
    	parents;
	"""
    ret = """
    water-filtered	water-outlet
    water-outlet	water-circuit
    water-circuit	water-main
    water-main	    water-supply
    """