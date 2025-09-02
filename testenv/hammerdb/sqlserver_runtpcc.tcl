diset connection mssqls_server sqlserver
diset connection mssqls_linux_server sqlserver
diset connection mssqls_port 1433
diset connection mssqls_uid sa
diset connection mssqls_pass Password!
diset connection mssqls_encrypt_connection false
diset tpcc mssqls_count_ware 1
diset tpcc mssqls_num_vu 1
vuset vu 1
vurun
exit

