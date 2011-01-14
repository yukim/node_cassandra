#include <string>
#include <sstream>

#include <boost/algorithm/string.hpp>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "cassandra.h"

using namespace std;
using namespace v8;

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace org::apache::cassandra;

void Client::Initialize(Handle<Object> target)
{
    HandleScope scope;

    Local<FunctionTemplate> t = FunctionTemplate::New(New);
    t->SetClassName(v8::String::New("Client"));
    t->InstanceTemplate()->SetInternalFieldCount(1);

    // Client.prototype.login
    NODE_SET_PROTOTYPE_METHOD(t, "login", Login);

    // Client.prototype.get
    NODE_SET_PROTOTYPE_METHOD(t, "get", Get);
    // Client.prototype.count
    NODE_SET_PROTOTYPE_METHOD(t, "count", Count);
    // Client.prototype.multiget
    NODE_SET_PROTOTYPE_METHOD(t, "multiget", MultiGet);
    // Client.prototype.multicount
    NODE_SET_PROTOTYPE_METHOD(t, "multicount", MultiCount);
    // Client.prototype.insert
    NODE_SET_PROTOTYPE_METHOD(t, "insert", Insert);
    // Client.prototype.remove
    NODE_SET_PROTOTYPE_METHOD(t, "remove", Remove);

    // Cassandra.prototype.clusterName 
    t->PrototypeTemplate()->SetAccessor(String::NewSymbol("clusterName"), ClusterNameGetter);
    // Cassandra.prototype.version 
    t->PrototypeTemplate()->SetAccessor(String::NewSymbol("version"), VersionGetter);

    // var cassandra = require('cassandra');
    // var client = new cassandra.Client();
    target->Set(String::NewSymbol("Client"), t->GetFunction());

    // ConsistencyLevel
    ConsistencyLevel level;

    Local<ObjectTemplate> cl = ObjectTemplate::New();
    cl->SetInternalFieldCount(1);
    cl->Set(String::NewSymbol("ONE"), Integer::New(level.ONE));
    target->Set(String::NewSymbol("ConsistencyLevel"), cl->NewInstance());
}

/**
 * constructor function
 *
 * function Client(keyspace, hosts, options) {
 *   ...
 * }
 */
Handle<Value> Client::New(const Arguments& args)
{
    HandleScope scope;
    Client *client;

    String::Utf8Value keyspace(args[0]);

    Handle<Value> hosts(args[1]);
    if (hosts->IsArray()) {
    } else {
        String::AsciiValue host(hosts->ToString());
        client = new Client(*keyspace, *host);
    }
    client->Wrap(args.This());

    return args.This();
}

/**
 * Client.protorype.login = function (username, password) {
 * }
 */
Handle<Value> Client::Login(const Arguments& args)
{
    HandleScope scope;
    Client *client = ObjectWrap::Unwrap<Client>(args.This());

    String::Utf8Value username(args[0]);
    String::Utf8Value password(args[1]);
    try {
        client->login(*username, *password);
    } catch (AuthenticationException e) {
    } catch (AuthorizationException e) {
    }
    return Undefined();
}

/**
 * Client.protorype.clusterName
 */
Handle<Value> Client::ClusterNameGetter(Local<String> property, const AccessorInfo& info)
{
    HandleScope scope;
    Client *client = ObjectWrap::Unwrap<Client>(info.This());
    return scope.Close(String::New(client->describe_cluster_name().c_str()));
}

/**
 * Client.protorype.version
 */
Handle<Value> Client::VersionGetter(Local<String> property, const AccessorInfo& info)
{
    HandleScope scope;
    Client *client = ObjectWrap::Unwrap<Client>(info.This());
    return scope.Close(String::New(client->describe_version().c_str()));
}

/**
 *
 * Client.prototype.get = function (column_family, key, column, sub_column, opts) {
 * };
 */
Handle<Value> Client::Get(const Arguments& args)
{
    HandleScope scope;

    Local<Value> key(args[1]);
    Handle<Value> obj = Client::MultiGet(args);
    return scope.Close(obj->ToObject()->Get(key));
}

/**
 *
 * Client.prototype.count = function (column_family, key, column, sub_column, opts) {
 * };
 */
Handle<Value> Client::Count(const Arguments& args)
{
    HandleScope scope;

    Local<Value> key(args[1]);
    Handle<Value> obj = Client::MultiCount(args);
    return scope.Close(obj->ToObject()->Get(key));
}

/**
 *
 * Client.prototype.multicount = function (column_family, keys, column, sub_column, opts) {
 * };
 */
Handle<Value> Client::MultiCount(const Arguments& args)
{
    HandleScope scope;
    Client *client = ObjectWrap::Unwrap<Client>(args.This());

    String::Utf8Value column_family(args[0]);
    Local<Value> keys(args[1]);

    vector<string> _keys;
    if (keys->IsArray())
    {
        // [key1, key2, ...]
        Local<Array> key_array = Local<Array>::Cast(keys);
        for (int i = 0, max = key_array->Length(); i < max; i++)
        {
            String::Utf8Value key(key_array->Get(i));
            _keys.push_back(*key);
        }
    }
    else
    {
        // single key
        String::Utf8Value key(keys);
        _keys.push_back(*key);
    }

    if (args.Length() > 3)
    {
        // super_column -> column
    }
    else if (args.Length() > 2)
    {
    }

    // execute query
    map<string, int32_t> result;
    try
    {
        result = client->multiget_count(_keys, *column_family, "");
    }
    catch (NotFoundException e)
    {
        // when data cannot be found, NotFoundException is thrown.
        return ThrowException(Exception::Error(
           String::New("A specific column was requested that does not exist")));
    }
    catch (InvalidRequestException e)
    {
        return ThrowException(Exception::Error(String::New(e.why.c_str())));
    }

    // construct return value
    Handle<ObjectTemplate> data_templ = ObjectTemplate::New();
    data_templ->SetInternalFieldCount(0);

    map<string, int32_t>::iterator it = result.begin();
	while (it != result.end())
	{
        string key((*it).first);
        int32_t count((*it).second);

        data_templ->Set(String::NewSymbol(key.c_str()), Number::New(count));
        ++it;
    }

    Local<Object> data = data_templ->NewInstance();
    return scope.Close(data);
}

/**
 *
 * Client.prototype.multiget = function (column_family, keys, column, sub_column, opts) {
 * };
 */
Handle<Value> Client::MultiGet(const Arguments& args)
{
    HandleScope scope;
    Client *client = ObjectWrap::Unwrap<Client>(args.This());

    String::Utf8Value column_family(args[0]);
    Local<Value> keys(args[1]);

    vector<string> _keys;
    if (keys->IsArray())
    {
        // [key1, key2, ...]
        Local<Array> key_array = Local<Array>::Cast(keys);
        for (int i = 0, max = key_array->Length(); i < max; i++)
        {
            String::Utf8Value key(key_array->Get(i));
            _keys.push_back(*key);
        }
    }
    else
    {
        // single key
        String::Utf8Value key(keys);
        _keys.push_back(*key);
    }

    if (args.Length() > 3)
    {
        // super_column -> column
    }
    else if (args.Length() > 2)
    {
    }

    // execute query
    map<string, vector<ColumnOrSuperColumn> > result;
    try
    {
        result = client->multiget_slice(_keys, *column_family, "");
    }
    catch (NotFoundException e)
    {
        // when data cannot be found, NotFoundException is thrown.
        return ThrowException(Exception::Error(
           String::New("A specific column was requested that does not exist")));
    }
    catch (InvalidRequestException e)
    {
        return ThrowException(Exception::Error(String::New(e.why.c_str())));
    }

    // construct return value
    Handle<ObjectTemplate> data_templ = ObjectTemplate::New();
    data_templ->SetInternalFieldCount(0);

    map<string, vector<ColumnOrSuperColumn> >::iterator it = result.begin();
	while (it != result.end())
	{
        vector<ColumnOrSuperColumn> data = (*it).second;
        vector<ColumnOrSuperColumn>::iterator data_it = data.begin();

        Handle<ObjectTemplate> col_templ = ObjectTemplate::New();
        col_templ->SetInternalFieldCount(0);
        while (data_it != data.end())
        {
            ColumnOrSuperColumn cosc = *data_it;
            if (cosc.__isset.super_column == true)
            {
                // super column
                SuperColumn sc = cosc.super_column;
                vector<Column>::iterator col_it = sc.columns.begin();

                Handle<ObjectTemplate> sc_templ = ObjectTemplate::New();
                sc_templ->SetInternalFieldCount(0);
                while (col_it != sc.columns.end())
                {
                    Column col = *col_it;
                    sc_templ->Set(String::New(col.name.c_str()), String::New(col.value.c_str()));
                    ++col_it;
                }
                col_templ->Set(String::New(sc.name.c_str()), sc_templ->NewInstance());
            }
            else
            {
                // standard
                Column col = cosc.column;
                col_templ->Set(String::New(col.name.c_str()), String::New(col.value.c_str()));
            }
            ++data_it;
        }

        string name((*it).first);
        data_templ->Set(String::NewSymbol(name.c_str()), col_templ->NewInstance());
        ++it;
    }

    Local<Object> column = data_templ->NewInstance();

    return scope.Close(column);
}

/**
 *
 * Client.prototype.insert = function (column_family, key, values, options) {
 * };
 */
Handle<Value> Client::Insert(const Arguments& args)
{
    HandleScope scope;
    Client *client = ObjectWrap::Unwrap<Client>(args.This());

    String::Utf8Value column_family(args[0]);
    String::Utf8Value key(args[1]);
    Local<Object> values = args[2]->ToObject();

    map<string, map<string, vector<Mutation> > > mutations_map;
    vector<Mutation> mutation_list;

    Local<Array> props = values->GetPropertyNames();
    for (int i = 0, max = props->Length(); i < max; i++)
    {
        Mutation mutation;
        ColumnOrSuperColumn cosc;

        String::Utf8Value propName(props->Get(i));
        Handle<Value> propValue = values->Get(props->Get(i));
        if (propValue->IsObject())
        {
            SuperColumn sc;
            sc.name = *propName;

            // super column
            Local<Array> propVals = propValue->ToObject()->GetPropertyNames();
            for (int j = 0, max2 = propVals->Length(); j < max2; j++)
            {
                // sub columns
                String::Utf8Value column_name(propVals->Get(j));
                String::Utf8Value value(propValue->ToObject()->Get(propVals->Get(j)));

                Column col = client->createColumn(*column_name, *value);

                sc.columns.push_back(col);
            }

            cosc.super_column = sc;
            cosc.__isset.super_column = true;
        }
        else
        {
            // standard column
            String::Utf8Value value(propValue);
            Column col = client->createColumn(*propName, *value);

            cosc.column = col;
            cosc.__isset.column = true;
        }
        mutation.column_or_supercolumn = cosc;
        mutation.__isset.column_or_supercolumn = true;
        mutation_list.push_back(mutation);
    }

    mutations_map[*key][*column_family] = mutation_list;
    try {
        client->batch_mutate(mutations_map);
    } catch (InvalidRequestException e) {
        return ThrowException(Exception::Error(String::New(e.why.c_str())));
    }
    return Undefined();
}

/**
 *
 * Client.prototype.remove = function (column_family, key, column, sub_column) {
 * };
 */
Handle<Value> Client::Remove(const Arguments& args)
{
    HandleScope scope;
    Client *client = ObjectWrap::Unwrap<Client>(args.This());

    String::Utf8Value column_family(args[0]);
    String::Utf8Value key(args[1]);

    map<string, map<string, vector<Mutation> > > mutations_map;
    vector<Mutation> mutation_list;
    if (args.Length() > 2)
    {
        Local<Object> values = args[2]->ToObject();
        if (args[2]->IsObject())
        {
            Local<Object> values = args[2]->ToObject();
            Local<Array> props = values->GetPropertyNames();
            for (int i = 0, max = props->Length(); i < max; i++)
            {
                String::Utf8Value propName(props->Get(i));
                Handle<Value> propValue = values->Get(props->Get(i));
                if (propValue->IsObject())
                {
                    // super column
                    printf("super\n");
                }
                else if (propValue->IsArray());
                {
                    // standard column
                    String::Utf8Value value(propValue);
                    printf("standard: %s\n", *value);
                }
            }
        }
        else
        {
            String::Utf8Value column_name(args[2]);

            Deletion del;
            del.timestamp = Timestamp::Now();
            del.predicate.column_names.push_back(*column_name);
            del.predicate.__isset.column_names = true;

            Mutation mutation;
            mutation.deletion = del;
            mutation.__isset.deletion = true;

            mutation_list.push_back(mutation);
        }
    }
    else
    {
        // remove entire row
        Deletion del;
        del.timestamp = Timestamp::Now();

        Mutation mutation;
        mutation.deletion = del;
        mutation.__isset.deletion = true;

        mutation_list.push_back(mutation);
    }

    mutations_map[*key][*column_family] = mutation_list;
    try {
        client->batch_mutate(mutations_map);
    } catch (InvalidRequestException e) {
        return ThrowException(Exception::Error(String::New(e.why.c_str())));
    }

    return Undefined();
}

/**
 * constructor
 */
Client::Client(const string &keyspace, const string &hosts, bool framed_transport)
{
    string::size_type pos = hosts.find_first_of(':');
    string host = hosts.substr(0, pos);
    string tmp_port = hosts.substr(pos + 1);
    int port;
    istringstream int_stream(tmp_port);
    int_stream >> port;

    boost::shared_ptr<TTransport> socket(new TSocket(host, port));
    boost::shared_ptr<TTransport> transport;
    if (framed_transport) 
    {
        transport = boost::shared_ptr<TTransport>(new TFramedTransport(socket));
    }
    else
    {
        transport = boost::shared_ptr<TTransport>(new TBufferedTransport(socket));
    }
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    thrift_client_ = new(std::nothrow) CassandraClient(protocol);

    transport->open();

    ConsistencyLevel level;
    default_cl_ = level.ONE;

    thrift_client_->set_keyspace(keyspace);
    thrift_client_->describe_cluster_name(cluster_name_);
    thrift_client_->describe_version(version_);

    keyspace_.assign(keyspace);

    discover_nodes();
}

Client::~Client()
{
    delete thrift_client_;
}

/**
 * Returns the name of the cluster
 *
 * @return name of the cluster
 */
string Client::describe_cluster_name()
{
    return cluster_name_;
}

string Client::describe_version()
{
    return version_;
}

vector<KsDef> Client::describe_keyspaces()
{
    vector<KsDef> ret;
    thrift_client_->describe_keyspaces(ret);
    return ret;
}

vector<TokenRange> Client::describe_ring()
{
    vector<TokenRange> range;
    thrift_client_->describe_ring(range, keyspace_);
    return range;
}

void Client::discover_nodes()
{
    vector<TokenRange> range = describe_ring();
    vector<TokenRange>::iterator range_it = range.begin();
    while (range_it != range.end())
    {
        vector<string> endpoints = (*range_it).endpoints;
        vector<string>::iterator endpoints_it = endpoints.begin();
        while (endpoints_it != endpoints.end())
        {
            servers_.insert(*endpoints_it);
            endpoints_it++;
        }

        range_it++;
    }
}

void Client::login(const string &user, const string &password)
{
    AuthenticationRequest auth;
    auth.credentials["username"] = user;
    auth.credentials["password"] = password;

    thrift_client_->login(auth);
}

map<string, vector<ColumnOrSuperColumn> > Client::multiget_slice(const vector<string> &keys, const string &column_family, const string &super_column_name)
{
    // construct column parent
    ColumnParent cp;
    cp.column_family.assign(column_family);
    if (!super_column_name.empty()) 
    {
        cp.super_column.assign(super_column_name);
        cp.__isset.super_column= true;
    }

    // construct slice predicate
    SlicePredicate sp;
    sp.slice_range.start = "";
    sp.slice_range.finish = "";
    sp.__isset.slice_range = true;

    map<string, vector<ColumnOrSuperColumn> > ret;
    thrift_client_->multiget_slice(ret, keys, cp, sp, default_cl_);

    return ret;
}

map<string, int32_t> Client::multiget_count(const vector<string> &keys, const string &column_family, const string &super_column_name)
{
    // construct column parent
    ColumnParent cp;
    cp.column_family.assign(column_family);
    if (!super_column_name.empty()) 
    {
        cp.super_column.assign(super_column_name);
        cp.__isset.super_column= true;
    }

    // construct slice predicate
    SlicePredicate sp;
    sp.slice_range.start = "";
    sp.slice_range.finish = "";
    sp.__isset.slice_range = true;

    map<string, int32_t> result;
    thrift_client_->multiget_count(result, keys, cp, sp, default_cl_);

    return result;
}

void Client::batch_mutate(const map<string, map<string, vector<Mutation> > > &mutation_map)
{
    thrift_client_->batch_mutate(mutation_map, default_cl_);
}

extern "C"
void init(Handle<Object> target)
{
    HandleScope scope;
    Client::Initialize(target);
}
