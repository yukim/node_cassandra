#ifndef _CASSANDRA_H_
#define _CASSANDRA_H_

#include <time.h>
#include <sys/time.h>
#include <v8.h>
#include <node.h>
#include <node_events.h>

#include "../gen-cpp/Cassandra.h"

using namespace org::apache::cassandra;

class Timestamp
{
    public:
        static int64_t Now()
        {
            struct timeval tv;
            gettimeofday(&tv, NULL);
            return (int64_t) tv.tv_sec * 1000000 + (int64_t) tv.tv_usec;
        }
};

class Client : public node::ObjectWrap
{
    public:
        static void Initialize(v8::Handle<v8::Object> target);

        // bridge functions
        static v8::Handle<v8::Value> New(const v8::Arguments& args);

        static v8::Handle<v8::Value> ClusterNameGetter(v8::Local<v8::String> property, const v8::AccessorInfo& info);
        static v8::Handle<v8::Value> VersionGetter(v8::Local<v8::String> property, const v8::AccessorInfo& info);

        static v8::Handle<v8::Value> Login(const v8::Arguments& args);
        static v8::Handle<v8::Value> Get(const v8::Arguments& args);
        static v8::Handle<v8::Value> Count(const v8::Arguments& args);
        static v8::Handle<v8::Value> MultiGet(const v8::Arguments& args);
        static v8::Handle<v8::Value> MultiCount(const v8::Arguments& args);
        static v8::Handle<v8::Value> Insert(const v8::Arguments& args);
        static v8::Handle<v8::Value> Remove(const v8::Arguments& args);

        Client(const std::string &keyspace, const std::string &hosts, bool  = true);
        ~Client();

        void login(const std::string &user, const std::string &password);

        std::map<std::string, std::vector<ColumnOrSuperColumn> > multiget_slice(const std::vector<std::string> &keys, const std::string &column_family, const std::string &super_column_name);
        std::map<std::string, int32_t> multiget_count(const std::vector<std::string> &keys, const std::string &column_family, const std::string &super_column_name);
        //std::vector<KeySlice> get_range_slices(const std::string &column_family, const std::string &super_column_name, const SlicePredicate& predicate, const KeyRange& range);
        //std::vector<KeySlice> get_indexed_slices(const std::string &column_family, const std::string &super_column_name, const IndexClause& index_clause, const SlicePredicate& column_predicate);

        void batch_mutate(const std::map<std::string, std::map<std::string, std::vector<Mutation> > > &mutation_map);

        std::string describe_cluster_name();
        std::string describe_version();
        std::vector<KsDef> describe_keyspaces();
        std::vector<TokenRange> describe_ring();
        /**
        KeDef describe_keyspace(const std::string& keyspace);
        std::string describe_partitioner();
        std::vector<std::string> describe_splits(const std::string& cfName, const std::string& start_token, const std::string& end_token, const int32_t keys_per_split);
        std::map<std::string, std::vector<std::string> > describe_schema_versions();
        std::string describe_snitch();
        **/

        /*
        Mutation createInsertMutation(const std::string &umn_, const std::string &value)
        {
        }*/

        Column createColumn(const std::string &name, const std::string &value)
        {
            Column col;
            col.name = name;
            col.value = value;
            col.timestamp = Timestamp::Now();

            return col;
        }

    private:
        void discover_nodes();

        bool is_super(const std::string &cf_name);

        std::string keyspace_;
        std::string cluster_name_;
        std::string version_;
        std::set<std::string> servers_;

        // default write consistency level
        ConsistencyLevel::type default_cl_;

        // default read consistency level
        ConsistencyLevel::type default_read_cl_;

        CassandraClient *thrift_client_;
};

#endif
