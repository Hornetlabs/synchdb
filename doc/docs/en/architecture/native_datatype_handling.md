# Native Data Type Handling

## **Handling Native Data Types**

The image below shows the list of supported native data types and how SynchDB groups them together based on their nature (or category). For example, the numeric group contains all integer or float data types that are numeric in nature. An error is raised if a numeric data contains non-numeric characters. Likewise, different groups of data types requires a sepcific format of data in order to apply.

![img](../../images/synchdb-native-types5.jpg)

Now that DML Converter knows how to produce the data for these supported native data types on the PostgreSQL side, it then looks at the DBZ metadata to learn how the source data is represented. This is needed because Debezium engine may encode the payload data to pack more information that requires decoding prior to processing the data, or use a structure to represent complex data types like Geometry. Without knowing how Debezium represents the data, the data processing is likely to produce undesired results, causing PostgreSQL to error during apply. Below is the list of formatting types that Debezium could represent a payload data with:

![img](../../images/synchdb-dbztype.jpg)

With these 2 pieces of information, DML Converter knows what the input looks like and what the output should look like. It will select the best handler from its function matrix to process the data. For example, if destination type is `FLOAT4`, and source data type is formatted as `DBZTYPE_BYTES`, the function `handle_base64_to_numeric()` will be selected to process the data. The selected function is responsible for decode the binary input and compute it as a numeric. 