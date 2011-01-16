
About
---------

node_cassandra is node.js addon for Apache Cassandra(http://cassandra.apache.org).

node_cassandra wraps thrift c++ client with nice javascript API.

Currently, only cassandra 0.7 is supported.

Requirement
-------------

To install and run, you need to have thrift 0.5.0 installed.

Install
---------

$ git clone https://yukim@github.com/yukim/node_cassandra.git
$ cd node_cassandra

then,

$ node-waf configure install

or

$ npm install .

I will push to npm repo later.

Usage
---------

Example usage::

  var cassandra = require('cassandra');
  var client = new cassandra.Client("Keyspace", "host:port");
  var CL = cassandra.ConsistencyLevel;

  client.consistencyLevel({
    write: CL.ONE, read: CL.ONE
  });

  client.insert("ColumnFamily", "key", {foo: "bar"});

  var data = client.get("ColumnFamily", "key");

For more detailed example, see test/test.js.

Limitation
------------

Following APIs are not yet supported.

* get_range_slice
* get_index_slice
* truncate
* system_* (schema modification APIs)
