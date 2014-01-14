Hecuba
======

There are few widely used Apache Cassandra clients and Hecuba is the abstraction over those clients so that you can switch between those easily.

Hecuba is currently in use in Wize Commerce and is being actively developed. 

Why Hecuba
==========
In greek mythology Hecuba (http://en.wikipedia.org/wiki/Hecuba) is the mother of Hector (http://en.wikipedia.org/wiki/Hector) and Cassandra (http://en.wikipedia.org/wiki/Cassandra) and grand mother of Asytanax (http://en.wikipedia.org/wiki/Astyanax). Whereas in NoSQL world Cassandra is a NoSQL storage, Hector and Astyanax are clients (Astyanax had initially spawned off of Hector) and Hecuba is the abstraction over Astyanax and Hector.    

Features
========

 - Hecuba supports all the regular methods to interact with Cassandra including gets, multigets, row and column slices, writes and deletes. 
 - Hecuba can be used to maintain your own secondary index without relying on Cassandra's implementation of secondary indexes. The main difference here is that Cassandra implementation tries to maintain the locality of the secondary indexes with the knowledge of the topology whereas hecuba implementation consider secondary index as another independent column family with pointers to the main table. 

 
