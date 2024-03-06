import buildscripts.moduleconfig as moduleconfig
import os

def configure(conf, env):
    root = os.path.dirname(__file__)

    if conf.CheckCXXHeader( "net-snmp/net-snmp-config.h" ):
        try:
            snmpFlags = env.ParseFlags("!net-snmp-config --agent-libs")
        except OSError, ose:
            # the net-snmp-config command was not found
            print( "WARNING: could not find or execute 'net-snmp-config', is it on the PATH?" )
            print( ose )
        else:
            snmp_module_name= moduleconfig.get_current_module_libdep_name('mongosnmp')
            env['SNMP_SYSLIBDEPS'] = snmpFlags['LIBS']
            del snmpFlags['LIBS']
            env.Append(**snmpFlags)
            env.Append(CPPDEFINES=["NETSNMP_NO_INLINE"],
                       MODULE_LIBDEPS_MONGOD=snmp_module_name)

    env['MONGO_BUILD_SASL_CLIENT'] = True
    if not conf.CheckLibWithHeader(
        "gsasl", "gsasl.h", "C", "gsasl_check_version(GSASL_VERSION);", autoadd=False):

        print("Could not find <gsasl.h> and gsasl library, required for enterprise build.")
        env.Exit(1)

    if not conf.CheckCXXHeader("gcrypt.h"):
        print("Could not find <gcrypt.h>, required for enterprise build.")
        env.Exit(1)

    sasl_server_module_name = moduleconfig.get_current_module_libdep_name('mongosaslservercommon')
    sasl_shell_module_name = moduleconfig.get_current_module_libdep_name('mongosaslshell')
    env.Append(MODULE_LIBDEPS_MONGOD=sasl_server_module_name,
               MODULE_LIBDEPS_MONGOS=sasl_server_module_name,
               MODULE_LIBDEPS_MONGOSHELL=sasl_shell_module_name)

    distsrc = env.Dir(root).Dir('distsrc')
    docs = env.Dir(root).Dir('docs')
    env.Append(MODULE_BANNERS=[
            distsrc.File('LICENSE.txt'),
            docs.File('MONGO-MIB.txt'),
            docs.File('mongod.conf'),
            docs.File('snmp.md')
            ])
