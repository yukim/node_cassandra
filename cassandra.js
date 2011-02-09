var thrift = require('thrift'),
    Cassandra = require('./gen-nodejs/Cassandra'),
    ttype = require('./gen-nodejs/cassandra_types');

var Client = function(host) {
  var pair = host.split(/:/);
  var connection = thrift.createConnection(pair[0], pair[1]);

  connection.on('error', function(err) {
    // do handle connection err
  });

  this.client_ = thrift.createClient(Cassandra, connection);

  this.defaultCL = {
    read: ttype.ConsistencyLevel.QUORUM,
    write: ttype.ConsistencyLevel.QUORUM
  };
}

Client.prototype.consistencyLevel = function() {
  if (arguments.length == 0) {
    return this.defaultCL;
  } else {
    var newCL = arguments[0];
    this.defaultCL.read = newCL.read || ttype.ConsistencyLevel.QUORUM;
    this.defaultCL.write = newCL.write || ttype.ConsistencyLevel.QUORUM;
  }
}

Client.prototype.cf = function(name) {
  return new ColumnFamily(this.client_, name);
}

var ColumnFamily = function(client, name) {
  this.client_ = client;
  this.name = name;
}

Client.prototype.get = function() {
  var args = Array.prototype.slice(arguments);
  var callback;
  if (typeof args[args.length - 1] === 'function') {
    callback = args.pop();
  }
  var keys = args.shift();
  var columns = args.shift() || {};
  var column_parent = new ttype.ColumnParent({
    column_family: this.name
  });
  if (columns) {
    if (typeof columns == "object") {
      // super columns
    }
  }
  // CassandraClient.prototype.multiget_slice = 
  //   function(
  //     keys,
  //     column_parent,
  //     predicate,
  //     consistency_level,callback)
  this.multiget_slice(
      keys,
      column_parent,
      null,

      callback);
}

exports.Client = Client;
exports.ConsistencyLevel = ttype.ConsistencyLevel;
