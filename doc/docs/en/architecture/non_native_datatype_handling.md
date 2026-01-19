# None-native Data Type Handling

## **Handling Non-Native Data Types**

It is possible that a table contains a column data type that is custom created by the user or created by another installed extension. In this case, it cannot be processed using tradition native data type handling because the type is most likely not supported natively. Instead, the DML Converter accesses the catalog, obtains the OID of the non-native data type, and looks up its "category" as defined in PostgreSQL. Below is a list of category supported by PostgreSQL as of version 17:

```
#define  TYPCATEGORY_INVALID	'\0'
#define  TYPCATEGORY_ARRAY		'A'
#define  TYPCATEGORY_BOOLEAN	'B'
#define  TYPCATEGORY_COMPOSITE	'C'
#define  TYPCATEGORY_DATETIME	'D'
#define  TYPCATEGORY_ENUM		'E'
#define  TYPCATEGORY_GEOMETRIC	'G'
#define  TYPCATEGORY_NETWORK	'I'
#define  TYPCATEGORY_NUMERIC	'N'
#define  TYPCATEGORY_PSEUDOTYPE 'P'
#define  TYPCATEGORY_RANGE		'R'
#define  TYPCATEGORY_STRING		'S'
#define  TYPCATEGORY_TIMESPAN	'T'
#define  TYPCATEGORY_USER		'U'
#define  TYPCATEGORY_BITSTRING	'V'
#define  TYPCATEGORY_UNKNOWN	'X'

```

The category tells DML Converter about the nature of the data type (numeric? string? datetime? ...etc) to help the converter select the right routine to process. For most cases, using type category paired with the DBZ metadata that describes how the input data payload is formatted is sufficient to select the right routine to process the data. However, in some cases, it may not be sufficient. For example, custom DATE, TIME, TIMESTAMP date types could all be categorized under `TYPCATEGORY_DATETIME`, so the converter does not know if it is working with a DATE, TIME or TIMESTAMP as each would produce different time formats. Currently, the covnerter looks for certain keywords from the data type name to identify. In the future, we may expose this part to let the user tell the converter exactly which routine to use should there be an ambiguity. Another example would be `TYPCATEGORY_USER` and `TYPCATEGORY_GEOMETRIC` which does not clearly indicate the data format. For these categories, the converter currently does not perform any further processing as it simply leaves the data payload as is. PostgreSQL may or may not reject such unprocessed data. This is why the transform feature next is important to give the DML converter a final chance to correct its data payload.