CREATE EXTENSION synchdb CASCADE;

\d

SELECT synchdb_add_conninfo('mysqlconn','127.0.0.1', 3306, 'mysqluser', 'mysqlpwd', 'inventory', 'postgres', 'inventory.orders,inventory.customers', 'mysql');
SELECT synchdb_add_conninfo('sqlserverconn','127.0.0.1', 1433, 'sa', 'Password!', 'testDB', 'postgres', '', 'sqlserver');
SELECT synchdb_add_conninfo('oracleconn','127.0.0.1', 1521, 'c##dbzuser', 'dbz', 'mydb', 'postgres', '', 'oracle');
SELECT synchdb_add_conninfo('errorconn','127.0.0.1', 1521, 'c##dbzuser', 'dbz', 'mydb', 'postgres', '', 'nonexist');

SELECT name, isactive, data->'hostname', data->'port', data->'user', data->'srcdb', data->'table', data->'connector' FROM synchdb_conninfo;

SELECT synchdb_add_extra_conninfo('mysqlconn', 'verufy_ca', 'keystore1', 'keystorepass', 'truststore1', 'truststorepass');
SELECT synchdb_add_extra_conninfo('sqlserverconn', 'verufy_ca', 'keystore2', 'keystorepass', 'truststore2', 'truststorepass');
SELECT synchdb_add_extra_conninfo('oracleconn', 'verufy_ca', 'keystore3', 'keystorepass', 'truststore3', 'truststorepass');

SELECT synchdb_del_extra_conninfo('mysqlconn');
SELECT synchdb_del_extra_conninfo('sqlserverconn');
SELECT synchdb_del_extra_conninfo('oracleconn');

SELECT name, isactive, data->'hostname', data->'port', data->'user', data->'srcdb', data->'table', data->'connector' FROM synchdb_conninfo;

SELECT data->'ssl_mode', data->'ssl_keystore', data->'ssl_truststore' FROM synchdb_conninfo;

SELECT synchdb_add_objmap('mysqlconn', 'table', 'ext_db1.ext_table1', 'pg_table1');
SELECT synchdb_add_objmap('mysqlconn', 'column', 'ext_db1.ext_table1.ext_column1', 'pg_column1');
SELECT synchdb_add_objmap('mysqlconn', 'datatype', 'int', 'bigint');
SELECT synchdb_add_objmap('mysqlconn', 'datatype', 'ext_db1.ext_table1.ext_column1', 'text');
SELECT synchdb_add_objmap('mysqlconn', 'transform', 'ext_db1.ext_table1.ext_column1', '''>>>>>'' || ''%d'' || ''<<<<<''');

SELECT synchdb_add_objmap('sqlserverconn', 'table', 'ext_db1.ext_table2', 'pg_table2');
SELECT synchdb_add_objmap('sqlserverconn', 'column', 'ext_db1.ext_table2.ext_column1', 'pg_column2');
SELECT synchdb_add_objmap('sqlserverconn', 'datatype', 'nchar', 'test');
SELECT synchdb_add_objmap('sqlserverconn', 'datatype', 'ext_db1.ext_table2.ext_column1', 'datetime');
SELECT synchdb_add_objmap('sqlserverconn', 'transform', 'ext_db1.ext_table2.ext_column1', '''>>>>>'' || ''%d'' || ''<<<<<''');

SELECT synchdb_add_objmap('oracleconn', 'table', 'ext_db1.ext_table3', 'pg_table3');
SELECT synchdb_add_objmap('oracleconn', 'column', 'ext_db1.ext_table3.ext_column1', 'pg_column3');
SELECT synchdb_add_objmap('oracleconn', 'datatype', 'number', 'bigint');
SELECT synchdb_add_objmap('oracleconn', 'datatype', 'ext_db1.ext_table3.ext_column1', 'varchar');
SELECT synchdb_add_objmap('oracleconn', 'transform', 'ext_db1.ext_table3.ext_column1', '''>>>>>'' || ''%d'' || ''<<<<<''');

SELECT synchdb_add_objmap('oracleconn', 'notexit', 'notexist', 'notexist');

SELECT * FROM synchdb_objmap;

SELECT synchdb_del_objmap('mysqlconn', 'table', 'ext_db1.ext_table1');
SELECT synchdb_del_objmap('mysqlconn', 'column', 'ext_db1.ext_table1.ext_column1');
SELECT synchdb_del_objmap('mysqlconn', 'datatype', 'int');
SELECT synchdb_del_objmap('mysqlconn', 'datatype', 'ext_db1.ext_table1.ext_column1');
SELECT synchdb_del_objmap('mysqlconn', 'transform', 'ext_db1.ext_table1.ext_column1');

SELECT synchdb_del_objmap('sqlserverconn', 'table', 'ext_db1.ext_table2');
SELECT synchdb_del_objmap('sqlserverconn', 'column', 'ext_db1.ext_table2.ext_column1');
SELECT synchdb_del_objmap('sqlserverconn', 'datatype', 'nchar');
SELECT synchdb_del_objmap('sqlserverconn', 'datatype', 'ext_db1.ext_table2.ext_column1');
SELECT synchdb_del_objmap('sqlserverconn', 'transform', 'ext_db1.ext_table2.ext_column1');

SELECT synchdb_del_objmap('oracleconn', 'table', 'ext_db1.ext_table3');
SELECT synchdb_del_objmap('oracleconn', 'column', 'ext_db1.ext_table3.ext_column1');
SELECT synchdb_del_objmap('oracleconn', 'datatype', 'number');
SELECT synchdb_del_objmap('oracleconn', 'datatype', 'ext_db1.ext_table3.ext_column1');
SELECT synchdb_del_objmap('oracleconn', 'transform', 'ext_db1.ext_table3.ext_column1');

SELECT * FROM synchdb_objmap;

SELECT synchdb_del_conninfo('mysqlconn');
SELECT synchdb_del_conninfo('sqlserverconn');
SELECT synchdb_del_conninfo('oracleconn');

SELECT name, isactive, data->'hostname', data->'port', data->'user', data->'srcdb', data->'table', data->'connector' FROM synchdb_conninfo;
SELECT * FROM synchdb_objmap;

DROP EXTENSION synchdb;
