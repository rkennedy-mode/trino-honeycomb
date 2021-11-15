# Honeycomb Trino Plugin

This [Trino plugin](https://trino.io/docs/current/develop/spi-overview.html) 
implements a custom connector for querying data from [Honeycomb](https://honeycomb.io/).

This code was written as part of the November 2021 Mode Hack Day.

## Honeycomb Catalog

A Honeycomb catalog can be added to a Trino server by doing the following:

1. Build the plugin JAR
2. Copy the plugin JAR to the Trino `plugin` directory
3. Create a `honeycomb.properties` file in the `etc/catalog` directory with `connector.name=honeycomb` 
   and `api.key` set to an appropriate Honeycomb API key.

With the above in place the Trino server should expose a new catalog named `honeycomb` with 
a `datasets` schema. Within the `datasets` schema should be one table per Honeycomb dataset.

```
% java -jar trino-cli-364-executable.jar --insecure --server localhost:8080
trino> show catalogs;
  Catalog  
-----------
 honeycomb 
 system    
(2 rows)

Query 20211115_193339_00000_g8ua9, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0.70 [0 rows, 0B] [0 rows/s, 0B/s]

trino> show schemas from honeycomb;
       Schema       
--------------------
 datasets           
 information_schema 
(2 rows)

Query 20211115_193348_00001_g8ua9, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
0.23 [2 rows, 36B] [8 rows/s, 158B/s]

trino> show tables from honeycomb.datasets;
                Table                 
--------------------------------------
 backend-services-staging             
 bes-traces                           
 bridge-server-development            
 bridge-server-production             
 bridge-server-staging                
 buildevents                          
 buildevents-jvm-mono                 
 buildevents-webapp                   
 buildevents_go_mono                  
 cdn-mode-com                         
 cdn-webapp-beta                      
 cdn-webapp-production                
 credguard-production                 
 credguard-staging                    

Query 20211115_193354_00002_g8ua9, FINISHED, 1 node
Splits: 19 total, 19 done (100.00%)
1.03 [55 rows, 1.98KB] [53 rows/s, 1.92KB/s]

```

## Current Status

The plugin implements much of the schema-related portions of the connector SPI. It does 
not yet implement the query-related portions of the SPI.

The provided connector can answer the following types of queries:

* `SHOW CATALOGS`
* `SHOW SCHEMAS FROM {catalog}`
* `SHOW TABLES FROM {schema}`
* `DESC {catalog}.{schema}.{table}`

It also allows much of the `information_schema` to work:

* `SELECT * FROM {catalog}.information_schema.columns`
