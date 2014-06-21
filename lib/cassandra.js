/**
 * Copyright 2011 Yuki Morishita<mor.yuki@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *:WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var sys = require('sys'),
    thrift = require('thrift'),
    Cassandra = require('../gen-nodejs/Cassandra'),
    ttype = require('../gen-nodejs/cassandra_types');

/**
 * @constructor
 * A connection to a Cassandra cluster.
 *
 * @param {String} host A "host:port" pair.
 * @api public
 *
 * @class
 * A connection to a Cassandra cluster.
 *
 * @description
 * <p>To connect to a Cassandra server, pass a "host:port" to the constructor then
 * use Client#connect.</p>
 *
 * <p>This is an EventEmitter that you may bind events to.
 * See the NodeJS documentation for information on EventEmitter.
 * (<a href='http://nodejs.org/docs/v0.4.10/api/all.html#events.EventEmitter'>Link</a>)</p>
 *
 * @example
 *   # Connect to given host and keyspace.
 *   var connection = new cassandra.Client("localhost:9160");
 *   connection.connect("keyspaceName");
 */
var Client = function(host) {
  var pair = host.split(/:/);

  /**
   * The hostname of the Cassandra server.
   * @type String
   */
  this.host = pair[0];

  /**
   * The port number as a string.
   * @type String
   */
  this.port = pair[1];

  /**
   * The default consistency level used in transactions.
   * @type Object
   *
   * @description
   * This is an object with <code>read</code> and <code>write</code> properties,
   * each being a ConsistencyLevel.
   */
  this.defaultCL = {
    read: ttype.ConsistencyLevel.QUORUM,
    write: ttype.ConsistencyLevel.QUORUM
  };
}
sys.inherits(Client, process.EventEmitter);

/**
 * @name Client#error
 * @event
 * @description
 * An error.
 *
 * @example
 *   # Trap errors.
 *   connection.on('error', function(err) {
 *     console.warn("An error occured.");
 *     console.warn(err);
 *   });
 */

/**
 * @name Client#keyspaceSet
 * @event
 * @description Called when a keyspace is set.
 */

/**
 * Connects to a Cassandra cluster.
 *
 * @param {String} keyspace The name of the keyspace to use (optional)
 * @param {Object} credentials If given, log into the cluster with the given username and password
 * @api public
 *
 * @example
 *   # Connect to `localhost:9160` with the keyspace name `keyspaceName`.
 *   var connection = new cassandra.Client("localhost:9160");
 *   connection.connect("keyspaceName");
 *
 * @example
 *   # This example uses the given login credentials.
 *   var connection = new cassandra.Client("localhost:9160");
 *   connection.connect("keyspaceName", {user: 'root', password: 'thrift'});
 *
 */
Client.prototype.connect = function() {
  var args = Array.prototype.slice.call(arguments);

  var keyspace_or_credential = args.shift();
  var credential = args.shift();
  if (keyspace_or_credential instanceof String ||
      typeof keyspace_or_credential === 'string') {
    // if first argument is string, then it is keyspace name
    this.keyspace = keyspace_or_credential;
  } else {
    credential = keyspace_or_credential;
  }

  this.ready = false;
  this.queue = [];

  this.connection = thrift.createConnection(this.host, this.port);
  this.connection.on('error', function(err) {
    this.emit('error', err);
  });
  this.thrift_client = thrift.createClient(Cassandra, this.connection);

  var self = this;
  this.connection.on('connect', function(err) {
    if (err) {
      self.emit('error', err);
      return;
    }

    // if credential is given, then call login
    if (credential) {
      self.thrift_client.login(
        new ttype.AuthenticationRequest(credential), function(err) {

          if (err) {
            self.emit('error', err);
            return;
          }

          // only when login is success, emit connected event
          self.ready = true;
          self.dispatch();
        });
    } else {
      self.ready = true;
      self.dispatch();
    }
  });

  // if keyspace is specified, use that ks
  if (this.keyspace) {
    this.use(this.keyspace, function(err) {
      if (err) {
        self.emit('error', err);
      }
    });
  }
};

/**
 * Sets which keyspace to use.
 *
 * @param {String} keyspace The name of the keyspace to use
 * @param {Function} callback (Optional) Callback function to be called after
 * @api public
 *
 * @description
 * You should probably use {connect} instead.
 *
 * @example
 *   var connection = new cassandra.Client("localhost:9160");
 *   connection.use("keyspaceName");
 *   connection.connect();
 */
Client.prototype.use = function(keyspace, callback) {
  var args = Array.prototype.slice.call(arguments);
  if (!this.ready) {
    this.queue.push([arguments.callee, args]);
    return;
  }

  this.keyspace = keyspace;

  var self = this;
  this.thrift_client.describe_keyspace(this.keyspace, function(err, ksdef) {
    if (err) {
      self.emit('error', err);
      return;
    }
    self.definition_ = ksdef;
    self.column_families_ = {};

    var i = ksdef.cf_defs.length;
    var cf;
    while (i--) {
      cf = ksdef.cf_defs[i];
      self.column_families_[cf.name] = cf;
    }

    self.thrift_client.set_keyspace(self.keyspace, function(err) {
      if (err) {
        selt.emit('error', err);
        return;
      }
      self.emit('keyspaceSet', self.column_families_);
    });
  });
};

/**
 * Set or get default consistency level.
 *
 * @param {Object} consistencyLevel (Optional) An object which has write and read consistency level.
 *           If given, sets default consistency level.
 * @api public
 *                         
 * @description
 * To set a consistency level, pass a consistencyLevel argument.
 *
 * @example
 *   # Sets the read/write consistency levels
 *   client.consistencyLevel({
 *     write: CL.ONE,
 *     read:  CL.ONE
 *   });
 *
 *   # Gets
 *   cl = client.consistencyLevel();  //=> { write: CL.ONE, read: CL.ONE }
 */
Client.prototype.consistencyLevel = function() {
  if (arguments.length == 0) {
    return this.defaultCL;
  } else {
    var newCL = arguments[0];
    this.defaultCL.read = newCL.read || ttype.ConsistencyLevel.QUORUM;
    this.defaultCL.write = newCL.write || ttype.ConsistencyLevel.QUORUM;
  }
};

/**
 * Creates a keyspace with the given name.
 *
 * @param {String} keyspaceName The name of the keyspace to be created
 * @param {Function} callback (Optional) Callback function to be called after
 * @api public
 *
 * @description
 * TODO: This method is a work in progress.
 *
 * @example
 *   client.addKeySpace('metrics', function() {
 *     // ...
 *   });
 */
Client.prototype.addKeySpace = function(ksdef, callback) {
  var args = Array.prototype.slice.call(arguments);
  if (!this.ready) {
    this.queue.push([arguments.callee, args]);
    return;
  }

  this.thrift_client.system_add_keyspace(new ttype.KsDef(ksdef), callback);
};

/**
 * Drops a given keyspace.
 *
 * @description
 * Deletes the keyspace with the given name and drops all data inside it.
 *
 * TODO: This method is a work in progress.
 *
 * @param {String} keyspaceName The name of the keyspace to be dropped
 * @param {Function} callback (Optional) Callback function to be called after
 * @api public
 *
 * @example
 *   client.dropKeySpace('metrics');
 */
Client.prototype.dropKeySpace = function(keyspace, callback) {
  var args = Array.prototype.slice.call(arguments);
  if (!this.ready) {
    this.queue.push([arguments.callee, args]);
    return;
  }

  this.thrift_client.system_drop_keyspace(keyspace, callback);
};

/**
 * Get column family to perform query or mutation.
 *
 * @param {String} name The name of the column family to retrieve
 * @return [ColumnFamily]
 * @api public
 *
 * @see ColumnFamily
 */
Client.prototype.getColumnFamily = function(name) {
  return new ColumnFamily(this, name);
};

/**
 * Closes the connection.
 *
 * @api public
 */
Client.prototype.close = function() {
  this.connection.end();
};

/**
 * @api private
 */
Client.prototype.dispatch = function() {
  if (this.ready) {
    if (this.queue.length > 0) {
      var next = this.queue.shift();
      next[0].apply(this, next[1]);
      this.dispatch();
    }
  }
};

/**
 * @constructor
 * A column family.
 *
 * @param {Client} client the Client
 * @param {String} name the name of the column family
 * @api public
 *
 * @class
 * A column family.
 *
 * @description
 * <p>This class can represent either a column family or a super column family. To
 * check if the object pertains to a super column family, use the isSuper
 * attribute.</p>
 *
 * <p>To retrieve a column family, use Client#getColumnFamily.</p>
 *
 * @property {String} name The name of the column family.
 * @property {String} column_type The type of the column family ('Super' or 'Standard').
 * @property {Boolean} ready The state of the column family.
 * @property {Boolean} isSuper True if the column family is a super column family.
 *
 * @example
 *   client = new cassandra.Client("localhost:9160");
 *   client.connect("keyspaceName");
 *
 *   family = client.getColumnFamily('pages');
 *   family.set(...);
 *
 * @see Client#getColumnFamily
 */
var ColumnFamily = function(client, name) {
  this.name = name;
  this.queue = [];
  this.ready = false;
  this.client_ = client;
  var self = this;
  this.client_.on('keyspaceSet', function(cfdef) {
    // check to see if column name is valid
    var cf = cfdef[self.name];
    if (!cf) {
      // column family does not exist
      self.client_.emit('error', new Error('Column Family ' + self.name + ' does not exist.'));
    }

    // copy all cfdef properties
    for (var prop in cf) {
      if (cf.hasOwnProperty(prop)) {
        self[prop] = cf[prop];
      }
    }

    self.isSuper = self.column_type === 'Super';
    self.ready = true;

    self.dispatch();
  });
};

/**
 * Retrieves data for a given key.
 *
 * @param keys row keys to fetch
 * @param columns (optional) which columns to retrieve
 * @param options (optional) valid params are start, finish, reversed, count
 * @param {Function} callback Callback function to be called after
 * @api public
 *
 * @description
 * You may supply options as an object literal. It can have one or more of the
 * following keys:
 *
 *   * start
 *   * finish
 *   * reversed (boolean)
 *   * count (number)
 *   * consistencyLevel (ConsistencyLevel)
 *
 */
ColumnFamily.prototype.get = function() {
  var args = Array.prototype.slice.call(arguments);
  if (!this.ready) {
    this.queue.push([arguments.callee, args]);
    return;
  }

  // last argument may be callback
  var callback;
  if (typeof args[args.length - 1] === 'function') {
    callback = args.pop();
  }
  // keys to get
  var keys = args.shift();
  // if only one key specified, turn it to array
  if (!(keys instanceof Array)) {
    keys = [keys];
  }

  var method_args = this.isSuper ? this.parseArgumentsForSuperCF_(args) : this.parseArgumentsForStandardCF_(args);
  var column_parent = method_args[0];
  var predicate = method_args[1];
  var cl = method_args[2] || this.client_.defaultCL.read;

  var self = this;
  this.client_.thrift_client.multiget_slice(
    keys, column_parent, predicate, cl,
    function(err, res) {
      if (err) {
        callback(err, obj);
      }

      // array -> obj
      var obj = {};
      var key, col, sub_col;
      for (key in res) {
        if (res.hasOwnProperty(key)) {
          obj[key] = {};
          var i = res[key].length;
          while (i--) {
            col = res[key][i].super_column;
            if (col) {
              // super
              obj[key][col.name] = {};
              var j = col.columns.length;
              while (j--) {
                sub_col = col.columns[j];
                obj[key][col.name][sub_col.name] = sub_col.value;
              }
            } else {
              // standard
              col = res[key][i].column;
              obj[key][col.name] = col.value;
            }
          }
        }
      }
      if (keys.length == 1) {
        obj = obj[keys[0]];
      }
      callback(err, obj);
    });
};

/**
 * Retrieves the column count of a given key.
 *
 * @param {Array} keys row keys to fetch. This can be a string or array.
 * @param {Object} columns (Optional) which columns to retrieve
 * @param {Object} options (Optional) options
 * @param {Function} callback Callback function to be called after
 *
 * @description
 * For description on the options parameter, see ColumnFamily#get.
 */
ColumnFamily.prototype.count = function() {
  var args = Array.prototype.slice.call(arguments);
  if (!this.ready) {
    this.queue.push([arguments.callee, args]);
    return;
  }

  // last argument may be callback
  var callback;
  if (typeof args[args.length - 1] === 'function') {
    callback = args.pop();
  }
  // keys to get
  var keys = args.shift();
  // if only one key specified, turn it to array
  if (!(keys instanceof Array)) {
    keys = [keys];
  }

  var method_args = this.isSuper ? this.parseArgumentsForSuperCF_(args) : this.parseArgumentsForStandardCF_(args);
  var column_parent = method_args[0];
  var predicate = method_args[1];
  var cl = method_args[2] || this.client_.defaultCL.read;

  this.client_.thrift_client.multiget_count(
    keys, column_parent, predicate, cl,
    function(err, res) {
      if (err) {
        callback(err, obj);
      }
      
      var obj = {};
      var key, count;
      for (key in res) {
        if (res.hasOwnProperty(key)) {
          obj[key] = res[key];
        }
      }
      if (keys.length == 1) {
        obj = obj[keys[0]];
      }
      callback(err, obj);
    });
};

/**
 * slice data
 * @api private
 */
ColumnFamily.prototype.slice = function() {
  this.client_.emit('error', new Error('slice(get_range_slices, get_indexed_slices) not supported.'));
};

/**
 * Sets data through insert or delete.
 *
 * @param {String} key
 * @param {Object} values
 * @param {Object} options
 * @param {Function} callback (Optional) Callback function to be called after
 * @api public
 *
 * @description
 * For super column families, values should be 2 levels down.
 *
 * @example
 *   # Sets some data for a normal column family.
 *   names = client.getColumnFamily('names');
 *   names.set("fruits", { "a": "apple" });
 *
 * @example
 *   # Sets some data for a super column family.
 *   metrics = client.getColumnFamily('metrics');
 *   if (metrics.isSuper) {
 *     metrics.set(
 *       "august_2011",
 *       {
 *         "10": { visits: 29, pageViews: 84 },
 *         "11": { visits: 14, pageViews: 29 }
 *       });
 *   }
 */
ColumnFamily.prototype.set = function() {
  var args = Array.prototype.slice.call(arguments);
  if (!this.ready) {
    this.queue.push([arguments.callee, args]);
    return;
  }

  var callback;
  if (typeof args[args.length - 1] === 'function') {
    callback = args.pop();
  }

  var key = args.shift();
  var values = args.shift() || {};
  var options = args.shift() || {};
  var cl = options.consistencyLevel || this.client_.defaultCL.write;
  var ts = new Date().getTime();

  var prop, value;
  var mutations = [], columns;
  if (this.isSuper) {
    // super
    for (prop in values) {
      if (values.hasOwnProperty(prop)) {
        columns = [];
        value = values[prop];
        for (var col in value) {
          columns.push(new ttype.Column({
            name: col,
            value: '' + value[col],
            timestamp: ts,
            ttl: null
          }));
        }
        // prop is super column name
        mutations.push(new ttype.Mutation({
          column_or_supercolumn: new ttype.ColumnOrSuperColumn({
            super_column: new ttype.SuperColumn({
              name: prop,
              columns: columns
            })
          })
        }));
      }
    }
  } else {
    // standard
    for (prop in values) {
      mutations.push(new ttype.Mutation({
        column_or_supercolumn: new ttype.ColumnOrSuperColumn({
          column: new ttype.Column({
            name: prop,
            value: '' + values[prop],
            timestamp: ts,
            ttl: null
          })
        })
      }));
    }
  }

  var mutation_map = {};
  mutation_map[key] = {};
  mutation_map[key][this.name] = mutations;

  this.client_.thrift_client.batch_mutate(mutation_map, cl, callback);
};

/**
 * Remove data.
 *
 * @param {String} key
 * @param {Object} columns optional. which columns to retrieve
 * @param {Object} options optional. valid params are start, finish, reversed, count
 * @param {Function} callback (Optional) Callback function to be called after
 * @api public
 *
 * @description
 * For description on the options parameter, see ColumnFamily.get.
 */
ColumnFamily.prototype.remove = function() {
  var args = Array.prototype.slice.call(arguments);
  if (!this.ready) {
    this.queue.push([arguments.callee, args]);
    return;
  }

  var callback;
  if (typeof args[args.length - 1] === 'function') {
    callback = args.pop();
  }

  var key = args.shift();

  var method_args = this.isSuper ? this.parseArgumentsForSuperCF_(args) : this.parseArgumentsForStandardCF_(args);
  var column_parent = method_args[0];
  var predicate = method_args[1];
  var cl = method_args[2] || this.client_.defaultCL.write;

  var ts = new Date().getTime();

  var mutations = [];
  mutations.push(new ttype.Mutation({
    deletion: new ttype.Deletion({
      timestamp: ts,
      super_column: column_parent.super_column,
      predicate: predicate.column_names ? predicate : null
    })
  }));

  var mutation_map = {};
  mutation_map[key] = {};
  mutation_map[key][this.name] = mutations;

  this.client_.thrift_client.batch_mutate(mutation_map, cl, callback);
};

/**
 * Removes all rows in a given column.
 *
 * @param {String} name The name.
 * @param {Function} callback (Optional) Callback function to be called after
 * @api public
 **/
ColumnFamily.prototype.truncate = function() {
  var args = Array.prototype.slice.call(arguments);
  if (!this.ready) {
    this.queue.push([arguments.callee, args]);
    return;
  }

  var callback = args.shift();
  this.client_.thrift_client.truncate(this.name, callback);
}

/**
 * dispatch queries when client is ready
 * @api private
 **/
ColumnFamily.prototype.dispatch = function() {
  if (this.ready) {
    if (this.queue.length > 0) {
      var next = this.queue.shift();
      next[0].apply(this, next[1]);
      this.dispatch();
    }
  }
};

/**
 * 
 * @api private
 * @param args
 * @return [ColumnParent, SlicePredicate, ConsistencyLevel]
 */
ColumnFamily.prototype.parseArgumentsForSuperCF_ = function(args) {
  var default_options = {
    start: '',
    finish: '',
    reversed: false,
    count: 100,
    consistencyLevel: null
  };
  var column_parent = {
    column_family: this.name
  };
  var predicate = {};

  var super_column_or_options = args.shift();
  var options = default_options;

  if (super_column_or_options) {
    // first argumet may be super column name
    if (super_column_or_options instanceof String ||
        typeof super_column_or_options === 'string') {
      column_parent.super_column = super_column_or_options;
      var columns_or_options = args.shift();
      if (columns_or_options) {
        var columns, options, option_name;
        if (typeof columns_or_options.slice === 'function') {
          // first argument is column name(s)
          columns = columns_or_options.slice();
          if (!(columns instanceof Array)) {
            columns = [columns];
          }
          predicate.column_names = columns;
          options = args.shift() || default_options;
        } else {
          // update default option with given value
          for (option_name in columns_or_options) {
            if (columns_or_options.hasOwnProperty(option_name)) {
              options[option_name] = columns_or_options[option_name];
            }
          }
          predicate.slice_range = new ttype.SliceRange(options);
        }
      } else {
        predicate.slice_range = new ttype.SliceRange(options);
      }
    } else {
      // update default option with given value
      for (option_name in super_column_or_options) {
        if (super_column_or_options.hasOwnProperty(option_name)) {
          options[option_name] = super_column_or_options[option_name];
        }
      }
      predicate.slice_range = new ttype.SliceRange(options);
    }
  } else {
    predicate.slice_range = new ttype.SliceRange(options);
  }

  return [new ttype.ColumnParent(column_parent),
         new ttype.SlicePredicate(predicate),
         options.consistencyLevel];
}

/**
 *
 * @api private
 * @param args
 * @return [ColumnParent, SlicePredicate, ConsistencyLevel]
 */
ColumnFamily.prototype.parseArgumentsForStandardCF_ = function(args) {
  var default_options = {
    start: '',
    finish: '',
    reversed: false,
    count: 100,
    consistencyLevel: null
  };
  var column_parent = {
    column_family: this.name
  };
  var predicate = {};

  var columns_or_options = args.shift();
  var options = default_options;

  if (columns_or_options) {
    var columns, options, option_name;
    if (typeof columns_or_options.slice === 'function') {
      // first argument is column name(s)
      columns = columns_or_options.slice();
      if (!(columns instanceof Array)) {
        columns = [columns];
      }
      predicate.column_names = columns;
      options = args.shift() || default_options;
    } else {
      // update default option with given value
      for (option_name in columns_or_options) {
        if (columns_or_options.hasOwnProperty(option_name)) {
          options[option_name] = columns_or_options[option_name];
        }
      }
      predicate.slice_range = new ttype.SliceRange(options);
    }
  } else {
    predicate.slice_range = new ttype.SliceRange(options);
  }

  return [new ttype.ColumnParent(column_parent),
         new ttype.SlicePredicate(predicate),
         options.consistencyLevel];
}

exports.Client = Client;

/**
 * @name ConsistencyLevel
 * @namespace Consistency levels.
 * ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL, ANY
 **/

exports.ConsistencyLevel = ttype.ConsistencyLevel;

/**
 * @name ConsistencyLevel.ONE
 * @constant One
 */

/**
 * @name ConsistencyLevel.QUORUM
 * @constant Quorum
 */

/**
 * @name ConsistencyLevel.LOCAL_QUORUM
 * @constant Local Quorum
 */

/**
 * @name ConsistencyLevel.EACH_QUORUM
 * @constant Each Quorum
 */

/**
 * @name ConsistencyLevel.ALL
 * @constant All
 */

/**
 * @name ConsistencyLevel.ANY
 * @constant Any
 */
