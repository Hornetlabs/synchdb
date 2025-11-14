#!/bin/bash

#this script takes the source and header files from IvorySQL repository that are
#necessary to build a oracle parser for synchdb compatible with  the postgresql
#major version synchdb is built with. Please note that IvorySQL must have the 
#matching major version for the oracle parser to work and you must have already
#run the following in IvorySQL repository to have all the required files:
#	
#	cd IvorySQL/src/backend/oracle_parser
#	make
#
set -euo pipefail

pgmajor=$1
ivorysql_root_dir=$2
synchdb_root_dir=$3
dest_dir=$synchdb_root_dir/src/backend/olr/oracle_parser$pgmajor.ivy

if [ $# -ne 3 ]; then
	echo "usage: ./prep_ora_parser.sh [pg major version] [path to ivorysql root repository] [path to synchdb root directory]"
	exit 
fi

if [ ! -d $ivorysql_root_dir ]; then
	echo "directory does not exist: $ivorysql_root_dir"
	exit 1
fi

if [ ! -d $synchdb_root_dir ]; then
    echo "directory does not exist: $synchdb_root_dir"
    exit 1
fi

if [ -d $dest_dir ]; then
	echo "destination directory already exists: $dest_dir"
	echo "abort..."
	exit 1
fi

mkdir $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/Makefile $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/liboracle_parser.c $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/ora_gram.c $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/meson.build $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/ora_gram.h $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/ora_gram.y $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/ora_gramparse.h $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/ora_kwlist_d.h $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/ora_keywords.c $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/ora_scan.c $dest_dir
cp $ivorysql_root_dir/src/backend/oracle_parser/ora_scan.l $dest_dir
cp $ivorysql_root_dir/src/backend/parser/check_keywords.pl $dest_dir

mkdir $dest_dir/include
mkdir $dest_dir/include/catelog
mkdir $dest_dir/include/nodes
mkdir $dest_dir/include/oracle_parser
mkdir $dest_dir/include/parser
mkdir $dest_dir/include/utils

cp $ivorysql_root_dir/src/include/catalog/pg_attribute_d.h $dest_dir/include/catelog
cp $ivorysql_root_dir/src/include/catalog/pg_attribute.h $dest_dir/include/catelog
cp $ivorysql_root_dir/src/include/nodes/primnodes.h $dest_dir/include/nodes
cp $ivorysql_root_dir/src/include/nodes/parsenodes.h $dest_dir/include/nodes
cp $synchdb_root_dir/src/backend/olr/oracle_parser17/include/nodes/nodetags_ext.h $dest_dir/include/nodes
cp $ivorysql_root_dir/src/include/catalog/pg_attribute.h $dest_dir/include/nodes
cp $ivorysql_root_dir/src/include/oracle_parser/* $dest_dir/include/oracle_parser
cp $ivorysql_root_dir/src/include/parser/parser.h $dest_dir/include/parser
cp $ivorysql_root_dir/src/include/utils/ora_compatible.h $dest_dir/include/utils

echo ">>>>> DONE SUCCESSFULY at $dest_dir <<<<<"

echo ">>>>> RUN THE FOLLOWING COMMANDS TO FINALIZE <<<<<"
echo "	patch -d $dest_dir -p1 < $synchdb_root_dir/src/backend/olr/oracle_parser_patches/Makefile.patch"
echo "	patch -d $dest_dir -p1 < $synchdb_root_dir/src/backend/olr/oracle_parser_patches/liboracle_parser.c.patch"
echo "	patch -d $dest_dir -p1 < $synchdb_root_dir/src/backend/olr/oracle_parser_patches/ora_compatible.h.patch"
echo "	patch -d $dest_dir -p1 < $synchdb_root_dir/src/backend/olr/oracle_parser_patches/ora_gram.y.patch"
echo "	mv oracle_parser$pgmajor.ivy oracle_parser$pgmajor"

echo ">>>>> RESOLVE PATCH AND COMPILATION ISSUES IF ANY <<<<<"

