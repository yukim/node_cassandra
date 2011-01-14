srcdir = '.'
blddir = 'build'
VERSION = '0.0.1'

def set_options(opt):
    opt.tool_options('compiler_cxx')

def configure(conf):
    conf.check_tool('compiler_cxx')
    conf.check_tool('node_addon')
    conf.check_cxx(lib='thrift', mandatory = True)
    conf.check_cxx(header_name='Thrift.h', mandatory = True)

def build(bld):
    obj = bld.new_task_gen('cxx', 'shlib', 'node_addon')
    obj.target = 'cassandra'
    obj.source = ['lib/cassandra.cc',
        'gen-cpp/Cassandra.cpp',
        'gen-cpp/cassandra_constants.cpp',
        'gen-cpp/cassandra_types.cpp']
    obj.cxxflags = '-Wall'
    obj.lib = ['thrift']
