DDF JDBC
========
This project depends on DDF and uses JDBC drivers to connect to SQL databases and provides a basic DDF implementation.
### Getting Started

This set of projects depends on ddf-core and requires it to be published before building this project. 

JDBC test suite
===============
A test suite has been developed to test any DDF implementation based on the DDF-JDBC project. 

Customizing the JDBC DDF Implementation for your database
=========================================================
1. Implement a Catalog (Documentation WIP)
2. Sub class JDBCDDFManager to make specific Manager implementation and override atleast the "getEngine" method.
3. Make a scalatest spec in the test source folder.
4. ...WIP


