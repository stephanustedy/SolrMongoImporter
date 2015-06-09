# Solr MongoDB Importer
Welcome to the Solr MongoDB Importer project. This project provides MongoDB support for the Solr Data Import Handler.

## Features
* Retrive data from a MongoDB collection
* Authenticate using MongoDB authentication
* Map Mongo fields to Solr fields wit mapMongoFields option (for accessing nested fields use "." (dot) as path separator eg.: *Params.Size*)
* Date conversion of field value to required format

## Classes

* **MongoDataSource** - Provides a MongoDB datasource
    * database (**required**) - The name of the data base you want to connect to
    * host (*optional* - default: localhost) - for replica set add comma separated values
    * port (*optional* - default: 27017) - for different ports in replica set add comma separated values
    * username (*optional*)
    * password (*optional*)
    * mapMongoFields (*optional* - default: true)


* **MongoEntityProcessor** - Use with the MongoDataSource to query a MongoDB collection
    * collection (**required**)
    * query (**required**)


* **MongoMapperTransformer** - Map MongoDB fields to your Solr schema
    * mongoField (**required**)
    * dateFormat (*optional*)

## Installation
1. Firstly you will need a copy of the Solr MongoDB Importer jar.

    Getting Solr MongoDB Importer
    1. [Download the JAR from GitHub](https://github.com/jsalcedo09/SolrMongoImporter/releases/download/v1.1.2/solr-mongo-importer-1.1.2.jar)
    2. Build your own using the ant build script. You will need the JDK installed as well as Ant with [Maven Ant Tasks](https://maven.apache.org/ant-tasks/) for downloading dependencies.


2. You will also need the [MongoDB Java driver 3.x JAR](http://mvnrepository.com/artifact/org.mongodb/mongo-java-driver)

3. Place both of these jar's in your Solr libaries folder (I put mine in 'lib' folder with the other jar's)

4. Add lib directives to your solrconfig.xml

```xml
<lib dir="./lib/" regex="solr-mongo-importer.*\.jar"/>
<lib dir="./lib/" regex="mongo-java-driver.*\.jar"/>
```

5. Add the below fields config in schema.xml
```
<field name="name" type="string" indexed="true" stored="true"/>
<field name="size" type="int" indexed="true" stored="true"/>
<field name="created" type="date" indexed="true" stored="true"/>
```

7. Add the below documents in mongo collection
```
use test;
db.products.update({"name":"Prod1"},{$set: { "attrib":{"size":1}, "deleted":"false"}, $currentDate: {lastmodified: true, created: true}}, {upsert: true, multi:true});
db.products.update({"name":"Prod2"},{$set: { "attrib":{"size":2}, "deleted":"false"}, $currentDate: {lastmodified: true, created: true}}, {upsert: true, multi:true});

db.products.update({"name":"Prod1"},{$set: {"deleted":"true"}, $currentDate: {lastmodified: true}}, {upsert: true, multi:true});
```

##Usage
Here is a sample data-config.xml showing the use of all components
```xml
<?xml version="1.0" encoding="UTF-8" ?>
<dataConfig>
  <dataSource name="MongoSource" type="MongoDataSource" database="test"/>
  <document name="products">
    <entity name="product"
           processor="MongoEntityProcessor"
           query='{$where: "${dataimporter.request.clean} != false || this.lastmodified > ISODate(\"${dataimporter.last_index_time}\")"}'
           collection="products"
           datasource="MongoSource"
           transformer="MongoMapperTransformer"
           mapMongoFields="true">
       <!--  If mongoField name and the field declared in schema.xml are same than no need to declare below.
           If not same than you have to refer the mongoField to field in schema.xml
          ( Ex: mongoField="EmpNumber" to name="EmployeeNumber"). -->
      <field column="_id" name="id"/>
      <field column="name" name="name" mongoField="name"/>
      <field column="size" name="size" mongoField="attrib.size"/>
      <field column="created" name="created" mongoField="created" dateFormat="yyyy-MM-dd HH:mm:ss"/>
      <field column="$skipDoc" mongoField="deleted"/>
      <field column="$deleteDocById" mongoField="_id"/>
    </entity>
  </document>
</dataConfig>
```