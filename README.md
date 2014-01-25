Hecuba
======

There are few widely used Apache Cassandra clients and Hecuba is the abstraction over those clients so that you can switch between those easily.

Hecuba is currently in use in Wize Commerce and is being actively developed. 

Why Hecuba
==========
In greek mythology [Hecuba](http://en.wikipedia.org/wiki/Hecuba) is the mother of [Hector](http://en.wikipedia.org/wiki/Hector) and [Cassandra](http://en.wikipedia.org/wiki/Cassandra) and grand mother of [Asytanax](http://en.wikipedia.org/wiki/Astyanax). Whereas in NoSQL world Cassandra is a NoSQL storage, Hector and Astyanax are clients (Astyanax had initially spawned off of Hector) and Hecuba is the abstraction over Astyanax and Hector.    

Features
========

- Hecuba supports all the regular methods to interact with Cassandra including gets, multigets, row and column slices, writes and deletes. 
- Hecuba can be used to maintain your own secondary index without relying on Cassandra's implementation of secondary indexes. The main difference here is that Cassandra implementation tries to maintain the locality of the secondary indexes with the knowledge of the topology whereas hecuba implementation consider secondary index as another independent column family with pointers to the main table. 

User Guide
==========

Usage
-----

### Initialization

```java
    CassandraParamsBean parameters = new CassandraParamsBean();
    parameters.setClusterName("Cluster Name");  // set the name of the cluster. You can find the name of the cluster from cassandra.yaml file
    parameters.setLocationURLs("node1:node2:node3"); // list of nodes in the cluster separated by ':'
    parameters.setThriftPorts("9180"); // port the above cassandra nodes listening to for client requests
    parameters.setKeyspace("MyKeyspace"); // the keyspace you need to connect to
    parameters.setCf("MyColumnFamily"); // the column family you need to connect to
    parameters.setKeyType("Long") // the java type of the keys of the cassandra cluster. This can be either Long or String.
    
    // initialize a client based on Astyanax
    //HecubaClientManager<String> cassandraManager = new AstyanaxBasedHecubaClientManager<String>(parameters, com.netflix.astyanax.serializer.StringSerializer.get());

    // or initialize a client based on Hector
    HecubaClientManager<String> cassandraManager = new HectorBasedHecubaClientManager<String>(parameters, me.prettyprint.cassandra.serializers.StringSerializer.get(), true);
```

### Write data 

```java
   // update a single column. Similarly, you can invoke updateDouble, updateLong, etc
   cassandraManager.updateString(key, columnName, columnValue);

   // update a single column with timestamp and ttl. By default each of these values are set to -1 so that the underlying client implementations will provide timestamps and set the TTL to never expire.
   cassandraManager.updateString(key, columnName, columnValue, timestamp, ttl);

   // update a whole row at once by passing a map of column names and values. Values can be strings, longs, booleans or Dates. Everything else has to be implement toString otherwise. 
   Map<String, Object> columns = ...
   cassandraManager.updateRow(key, columns);

   // update a whole row with ttls and timestamps
    Map<String, Long> timestamps = ...
	Map<String, Integer> ttls = ...
	cassandraManager.updateRow(key, columns, timestamps, ttls);
```

### Read data

```java

    // read all columns of a single row (single get)
    CassandraResultSet<K, String> columnValues = cassandraManager.readAllColumns(objectId);

    // read all columns of multiple rows (multi get)
    Set<Long> keysOfObjectsToBeRetrieved = new HashSet<Long>();
    CassandraResultSet<K, String> columnValues = cassandraManager.readAllColumns(keysOfObjectsToBeRetrieved);

    // read single column as string. 
    String stringColumnValue = cassandraManager.readString(key, stringColumnName);

    // read single column as double
    Double doubleValue = cassandraManager.readDouble(key, doubleColumnValue);

    // you can even pass in the default value so that this method returns it if there is no value found for that column inside Cassandra. You can similarly use readBoolean, readInteger methods. 
    Double doubleValue = cassandraManager.readDouble(key, doubleColumnValue, defaultDoubleValue);

    // read only a given set of columns for an object.
    List<String> columnsNamesToBeRetrieved = ...
    CassandraResultSet<K, String> columns = cassandraManager.readColumns(key, columnsNamesToBeRetrieved); 

    // similarly you can pass in a set of keys if you want to read columns of multiple keys.
```    

Current Limitations
===================

- Hecuba assumes all column names and values are strings. 


 
