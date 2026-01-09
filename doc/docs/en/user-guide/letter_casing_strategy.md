# Letter Casing Strategy

## Configure Letter Casing Strategy

Letter casing strategy represents how SynchDB should normalize source object names before applying them to destination. MySQL, SQL Server and PostgreSQL default all object names to lower case letters while Oracle defaults to upper case.  MySQL, Oracle and PostgreSQL allow object names to contain a mixture of upper and lower case letters, and consider each variant different while SQL Server treats them as all the same. 

It is possible to tell SynchDB how to handle the object names using `synchdb.letter_casing_strategy` GUC parameter with these possible values:

* lowercase: normalize all object names (table and column names) to lower case letters
* uppercase: normalize all object names (table and column names) to upper case letters
* asis: do not normalize, keep object names(table and column names) as is.

<<IMPORTANT>> synchdb.letter_casing_strategy does not affect data type names. It is always normalized to lower case in SynchDB. Connectors like SQL Server and Oracle express data type in upper case letters, so these are stored and displayed as lower case in SynchDB.

## How does Letter Casing Strategy Affect Transformation

You may wonder if SynchDB normalizes object names according to `synchdb.letter_casing_strategy`, how shall [object transformation](../../user-guide/object_mapping_rules/) be configured correctly to identify a foreign table or column to transform?

Object transformation identifies the original name from source database for transformation, so you have to configure the object name exactly as they appear in the source database, not the one normalized by SynchDB. 

If a match is found, SynchDB will prioritize the new value as identified by the object transformation and will not proceed to normalize it according to the letter casing strategy. Normalization occurs when there is no particular transformation rule defined for an object

For example, for an Oracle table 'TEST_TABLE' under database 'FREE' and schema 'DBZUSER', let's say we want to map it to "MY_TEST_TABLE" (all capital) in PostgreSQL and current synchdb.letter_casing_strategy is 'lowercase'. You will write the transform rule like this:

```sql

SELECT synchdb_add_objmap('mysqlconn','table','FREE.DBZUSER.TEST_TABLE','MY_TEST_TABLE');
```

Since we have transformation rule specifically for "FREE.DBZUSER.TEST_TABLE", this transformation will take precedence and transform it to 'MY_TEST_TABLE` as instructed. The letter casing strategy (lowercase) will not normalize it to lowercase.
