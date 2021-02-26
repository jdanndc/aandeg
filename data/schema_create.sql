
CREATE TABLE IF NOT EXISTS equip_class (
    ec_id VARCHAR(128) PRIMARY KEY,
    type VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS equip_class_depends (
    ec_id VARCHAR(128) NOT NULL,
    ec_id_parent VARCHAR(128) NOT NULL,
    depend_type VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_class (
    pc_id VARCHAR(128) PRIMARY KEY,
    type VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS prod_class_depends (
    pc_id VARCHAR(128) NOT NULL,
    ec_id_parent VARCHAR(128) NOT NULL,
    depend_type VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS store_class (
    sc_id VARCHAR(128) NOT NULL,
    type VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS store (
    s_id VARCHAR(128) NOT NULL,
    sc_id VARCHAR(128) NOT NULL,
    type VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS store_class_prod (
    sc_id VARCHAR(128) NOT NULL,
    pc_id VARCHAR(128) NOT NULL
);

CREATE TABLE IF NOT EXISTS incident_report (
    id SERIAL PRIMARY KEY,
    s_id VARCHAR(128) NOT NULL,
    ec_id VARCHAR(128) NOT NULL,
    type VARCHAR(32) NOT NULL,
    description VARCHAR(256) NOT NULL
);


