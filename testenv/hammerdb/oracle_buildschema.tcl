dbset db ora
diset connection system_user c##dbzuser
diset connection system_password dbz
diset connection instance "oracle:1521/FREE"
diset tpcc tpcc_user c##dbzuser
diset tpcc tpcc_pass dbz
diset tpcc tpcc_def_tab LOGMINER_TBS
buildschema
exit
