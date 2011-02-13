/**
 * This test assumes that your cassandra cluster is located at localhost/9160.
 *
 * In order to run this test, first create keyspace and cf with
 * DDL included with this test script.
 *
 * cassandra-cli --host localhost --batch < test.ddl
 */
var assert = require('assert'),
    cassandra = require('../cassandra');

// number of tests

// connect to cassandra
var client = new cassandra.Client('127.0.0.1:9160');

module.exports = {

  'test if ConsistencyLevel is exported properly': function() {
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
  },

  'test if operations on client works properly': function(beforeExit) {
    client.connect('node_cassandra_test');
    // or login if needed
    //client.connect('node_cassandra_test', {username: 'foo', password: 'bar'});

    var standard, superCF, notExistCF;
    standard = client.getColumnFamily('Standard');
    superCF = client.getColumnFamily('Super');
    //notExistCF = client.getColumnFamily('NotExistCF');

    //-------------------------------------
    // set
    // set one record to standard column family.
    standard.set('todd', {
      id: 1,
      first_name: 'Todd',
      last_name: 'Dahl',
      age: 24
    }, function(err) {
      assert.equal(err, null);
    });

    // make sure it is seted.
    standard.get('todd', function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        id: '1',
        first_name: 'Todd',
        last_name: 'Dahl',
        age: '24',
      });
    });
    // note that even though you set Number,
    // cassandra returns in String.

    // if you query for the key that doesn't exist, you will get empty object.
    standard.get('notexist', function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {});
    });
    /**
      notExistCF.get('notexist', function(err, res) {
      console.dir(err);
      assert.equal(res, null);
      });
      */

    // see if multiget works as expected.
    standard.set('jesse', {
      id: 2,
      first_name: 'Jesse',
      last_name: 'Pitman'
    });

    standard.get(['todd', 'jesse'], function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        todd: {
                id: '1',
        first_name: 'Todd',
        last_name: 'Dahl',
        age: '24',
              },
        jesse: {
                 id: '2',
        first_name: 'Jesse',
        last_name: 'Pitman'
               }
      });
    });

    // read operation with options.
    // valid options are:
    //   start: SliceRange start
    //   finish: SliceRange finish
    //   reversed: SliceRange reversed
    //   count: SliceRange count
    //
    //   ttl: column ttl (not yet)
    //   consistency_level: read consisteny level (not yet)
    //
    // specifying column names
    standard.get('todd', ['id', 'age'], function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        id: '1',
        age: '24',
      });
    });
    /*
    // specifying column names and CL
    var selected_result_with_cl_any = client.get('Standard', 'todd',
    ['id', 'age'],
    {consistency_level: CL.ANY});
    assert.deepEqual(selected_result_with_cl_any, {
    id: '1',
    age: '24',
    });
    */
    // count scan
    standard.get('todd', {count: 1}, function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        age: '24',
      });
    });
    // range scan
    standard.get('todd', {start: '', finish: 'age'}, function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        age: '24',
      });
    });
    // modifying consistency level not yet supported
    //client.set('Standard', 'jesse', {
    //  id: 2,
    //  first_name: 'Jesse',
    //  last_name: 'Pitman'
    //}, {
    //  consistency_level: CL.ALL
    //});

    // TTL is not supported yet.
    //client.set('Standard', 'foo', {
    //  bar: 'baz'
    //}, {
    //  ttl: 1000
    //});

    // counting
    // let's count number of cols
    standard.count('todd', function(err, res) {
      assert.equal(err, null);
      assert.equal(4, res);
    });

    // you can count colmns of multiple keys
    standard.count(['todd', 'jesse'], function(err, res){
      assert.equal(err, null);
      assert.deepEqual(res, {
        todd: 4,
        jesse: 3
      });
    });

    // super column
    superCF.set('edgar', {
      name:
    {first_name: 'Edgar', last_name: 'Sawyers'},
      address:
    {city: 'Madison', state: 'WI'}
    }, function(err) {
      assert.equal(err, null);
    });

    superCF.get('edgar', function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        name:
      {first_name: 'Edgar', last_name: 'Sawyers'},
        address:
      {city: 'Madison', state: 'WI'}
      });
    });

    superCF.get('edgar', 'address', function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        city: 'Madison',
        state: 'WI'
      });
    });

    // get only one column for certain key
    superCF.get('edgar', 'address', ['city'], function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        city: 'Madison'
      });
    });
    // TODO clarify
    superCF.get('edgar', 'address', {reversed: true, limit: 1}, function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        city: 'Madison',
        state: 'WI'
      });
    });

    // remove
    standard.remove('todd', 'id', function(err) {
      assert.equal(err, null);
    });
    standard.get('todd', function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        first_name: 'Todd',
        last_name: 'Dahl',
        age: '24',
      });
    });

    standard.remove('todd', ['first_name', 'last_name']);
    standard.get('todd', function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {
        age: '24',
      });
    });

    standard.remove('todd');
    standard.get('todd', function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {});
    });

    standard.remove('jesse');
    standard.get('jesse', function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {});
    });

    superCF.remove('edgar');
    superCF.get('edgar', function(err, res) {
      assert.equal(err, null);
      assert.deepEqual(res, {});
    });

    // close connection before exit
    beforeExit(function() { client.close(); });
  }
};
