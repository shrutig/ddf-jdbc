DDF JDBC
========
This project depends on DDF and uses JDBC drivers to connect to SQL databases and provides a basic DDF implementation.
### Getting Started
This set of projects depends on ddf-core and requires it to be published before building this project. To get ddf-core, clone DDF repo

```
$ git clone git@github.com:ddf-project/DDF.git
$ cd DDF
```

DDF can be installed by,
```
$ sbt publishLocal
```

Installing `ddf-jdbc` can be done by
```
$ git clone git@github.com:tuplejump/ddf-jdbc.git
$ cd ddf-jdbc
$ sbt publishLocal
```

This will publish four modules viz. ddf-jdbc,ddf-jdbc-test,ddf-jdbc-postgres,ddf-jdbc-aws

One published it may be used in any project via Maven or SBT as dependencies.

JDBC test suite
===============
A test suite has been developed to test any DDF implementation based on the DDF-JDBC project. 
This test suite has different behaviours. Particular behaviors can be added like the example [here] (https://github.com/tuplejump/ddf-jdbc/blob/master/postgres/src/test/scala/io/ddf/postgres/PostgresJdbcDDFSpec.scala)

Customizing the JDBC DDF Implementation for your database
=========================================================
1. Sub class JDBCDDFManager to make specific Manager implementation and override atleast the "getEngine" method.
2. Implement a Catalog optionally.
3. Sub class any Handlers optionally.
4. Make a scalatest spec in the test source folder. This spec will extend various Behaviors used for testing.
5. A template spec is [here] (https://github.com/tuplejump/ddf-jdbc/blob/master/postgres/src/test/scala/io/ddf/postgres/PostgresJdbcDDFSpec.scala)
6. Change build.sbt to include your implementation optionally, if you are using the same repo or a fork.

Using one of the JDBC Implementations
=====================================
1. Change the ddf-conf/ddf.ini file in the repo and go to the relevant section as per the implementation. Change the jdbcUrl, jdbcUser and jdbcPassword. 
2. Use DDFManager.get("engine",dataSourceDescriptor) to obtain an instance of a JDBCDDFManager.
3. Optionally set the "workspaceSchema" to a schema/namespace in your database to add write access. This will enable write operations on the database and also enable DDFManager.sql2ddf calls. Warning!! The sql2ddf calls create views on the workspaceSchema.

Using AWS Implementation
=====================================
Change the ddf-conf/ddf.ini file in the repo and go to the aws section.
 
1. Change the jdbcUrl, jdbcUser and jdbcPassword of the AWS Redshift database. 
2. Change redshiftClusterId, redshiftDatabase and redshiftIAMRoleARN(Amazon Resource Name (ARN)) to your corresponding credentials.
3. Change s3BucketName to the s3 bucket where you want to store any batch prediction or datasources, s3Key to the full dir name where you want to store batch prediction or datasources and change s3Region to the region where your s3 bucket is hosted.
4. Change awsAccessId and awsAccessKey according to your aws credentials.

Using Teradata Implementation
=====================================
Change the ddf-conf/ddf.ini file in the repo and change the teradata section.

Create a lib directory in the teradata folder and unzip the teradata jdbc driver jars downloaded from the Teradata Developer Exchange
