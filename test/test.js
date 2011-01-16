/**
 * This test assumes that your cassandra cluster is located at localhost/9160.
 *
 * In order to run this test, first create keyspace and cf with
 * DDL included with this test script.
 *
 * cassandra-cli --host localhost --batch < test.ddl
 */
var assert = require("assert"),
    cassandra = require("../build/default/cassandra");

// connect to cassandra
var client = new cassandra.Client("node_cassandra_test", "127.0.0.1:9160");

// login if needed
// client.login("user", "pass");

// make sure all consistency levels are exported
var CL = cassandra.ConsistencyLevel;
assert.deepEqual(CL, {
  ONE: 1,
  QUORUM: 2,
  LOCAL_QUORUM: 3,
  EACH_QUORUM: 4,
  ALL: 5,
  ANY: 6
});

// client configuration
// consistency level
// default is CL.QUORUM for both reads and writes
assert.deepEqual(client.consistencyLevel(), {
  write: CL.QUORUM,
  read: CL.QUORUM
});
// let's change default
client.consistencyLevel({
  write: CL.ONE,
  read: CL.ONE
});
// and check
assert.deepEqual(client.consistencyLevel(), {
  write: CL.ONE,
  read: CL.ONE
});

//-------------------------------------
// insert
// insert one record to standard column family.
client.insert("Standard", "todd", {
  id: 1,
  first_name: "Todd",
  last_name: "Dahl",
  age: 24
});

// make sure it is inserted.
var get_result = client.get("Standard", "todd");
assert.deepEqual(get_result, {
  id: "1",
  first_name: "Todd",
  last_name: "Dahl",
  age: "24",
});
// note that even though you inserted Number,
// cassandra returns in String.

// if you query for the key that doesn't exist, you will get empty object.
var empty = client.get("Standard", "notexist");
assert.deepEqual(empty, {});

// see if multiget works as expected.
client.insert("Standard", "jesse", {
  id: 2,
  first_name: "Jesse",
  last_name: "Pitman"
});

var multiget_result = client.multiget("Standard", ["todd", "jesse"]);
assert.deepEqual(multiget_result, {
  todd: {
    id: "1",
    first_name: "Todd",
    last_name: "Dahl",
    age: "24",
  },
  jesse: {
    id: "2",
    first_name: "Jesse",
    last_name: "Pitman"
  }
});

// read operation with options.
// valid options are:
//   start: SliceRange start
//   finish: SliceRange finish
//   reversed: SliceRange reversed
//   limit: SliceRange count
//
//   ttl: column ttl (not yet)
//   consistency_level: read consisteny level (not yet)
//
// specifying column names
var selected_result = client.get("Standard", "todd", ['id', 'age']);
assert.deepEqual(selected_result, {
  id: "1",
  age: "24",
});
// specifying column names and CL
var selected_result_with_cl_any = client.get("Standard", "todd",
    ['id', 'age'],
    {consistency_level: CL.ANY});
assert.deepEqual(selected_result_with_cl_any, {
  id: "1",
  age: "24",
});
// count scan
var count_result = client.get("Standard", "todd", {limit: 1});
assert.deepEqual(count_result, {
  age: "24",
});
// range scan
var range_result = client.get("Standard", "todd", {start: "", finish: "age"});
assert.deepEqual(range_result, {
  age: "24",
});
// reversed
var reversed_result = client.get("Standard", "todd", {reversed: true, limit: 2});
assert.deepEqual(reversed_result, {
  id: "1",
  last_name: "Dahl",
});
// modifying consistency level not yet supported
/*
client.insert("Standard", "jesse", {
  id: 2,
  first_name: "Jesse",
  last_name: "Pitman"
}, {
  consistency_level: CL.ALL
});
*/

// TTL is not supported yet.
/*
client.insert("Standard", "foo", {
  bar: "baz"
}, {
  ttl: 1000
});
*/

// counting
// let's count number of cols
var count_result = client.count("Standard", "todd");
assert.equal(4, count_result);

// you can count colmns of multiple keys
var multicount_result = client.multicount("Standard", ["todd", "jesse"]);
assert.deepEqual(multicount_result, {
  todd: 4,
  jesse: 3
});

// super column
client.insert("Super", "edgar", {
  name:
    {first_name: "Edgar", last_name: "Sawyers"},
  address:
    {city: "Madison", state: "WI"}
});

var sc_data = client.get("Super", "edgar");
assert.deepEqual(sc_data, {
  name:
    {first_name: "Edgar", last_name: "Sawyers"},
  address:
    {city: "Madison", state: "WI"}
});

sc_data = client.get("Super", "edgar", "address");
assert.deepEqual(sc_data, {
  city: "Madison",
  state: "WI"
});

sc_data = client.get("Super", "edgar", "address", ["city"]);
assert.deepEqual(sc_data, {
  city: "Madison"
});

sc_data = client.get("Super", "edgar", "address", {reversed: true, limit: 1});
assert.deepEqual(sc_data, {
  state: "WI"
});

// remove
setTimeout(function() {
  client.remove("Standard", "todd");
  var removed = client.get("Standard", "todd");
  assert.deepEqual(removed, {});

  client.remove("Standard", "jesse");
  removed = client.get("Standard", "jesse");
  assert.deepEqual(removed, {});

  client.remove("Super", "edgar");
  removed = client.get("Super", "edgar");
  assert.deepEqual(removed, {});
}, 100);

console.log("tests are ok");
