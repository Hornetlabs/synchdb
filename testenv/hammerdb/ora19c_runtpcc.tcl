dbset db ora
diset connection system_user DBZUSER
diset connection system_password dbz
diset connection instance "ora19c:1521/FREE"
diset tpcc tpcc_user DBZUSER
diset tpcc tpcc_pass dbz
diset tpcc tpcc_def_tab LOGMINER_TBS
vurun
exit

