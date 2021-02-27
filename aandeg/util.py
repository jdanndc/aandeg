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


def scratch_cte_query():
    # Common Table Expressions
    q = """
    WITH RECURSIVE parents AS (
	    SELECT
		    ec_id,
		    ec_id_parent
	    FROM
	    	equip_class_depends_2021_02_24_21_53_42
	    WHERE
	    	ec_id = 'device-ice-maker'
    	UNION
	    	SELECT
	    		e.ec_id,
    			e.ec_id_parent
    		FROM
			equip_class_depends_2021_02_24_21_53_42 e
    		INNER JOIN parents p ON p.ec_id_parent = e.ec_id
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

    foo = """
/* all products for a store, based on its store class */
select  scp.sc_id, s.s_id, scp.pc_id 
from store_class_prod scp 
inner join store s 
on scp.sc_id=s.sc_id and ( s.s_id = 'store-3')
"""

    foo = """
/* all unavailable equipment given one equipment fail */
select ecp.ec_id 
from equip_class_depends ecp 
where ec_id_parent 
in (select ec_id from incident_report ir where ir.s_id = 'store-1')    
    """

    foo = """
/* all unavailable products given equipment fail for store */
select distinct pcd.pc_id from prod_class_depends pcd 
where pcd.ec_id_parent  in (
select ecp.ec_id 
from equip_class_depends ecp 
where ec_id_parent 
in (select ec_id from incident_report ir where ir.s_id = 'store-1'))
    """
