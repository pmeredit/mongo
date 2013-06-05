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
        "sasl2",
        "sasl/sasl.h", "C",
        "sasl_version_info(0, 0, 0, 0, 0, 0);",
        autoadd=False):

        print("Could not find <sasl/sasl.h> and sasl library, required for enterprise build.")
        env.Exit(1)

    if conf.CheckLib(library="gssapi_krb5", autoadd=False):
        env['MONGO_GSSAPI_IMPL'] = "gssapi"
        env['MONGO_GSSAPI_LIB'] = "gssapi_krb5"
    elif "win32" == os.sys.platform:
        env['MONGO_GSSAPI_IMPL'] = "sspi"
    else:
        print("Could not find gssapi_krb5 library nor Windows OS, required for enterprise build.")
        env.Exit(1)

    sasl_server_module_name = moduleconfig.get_current_module_libdep_name('mongosaslservercommon')
    env.Append(MODULE_LIBDEPS_MONGOD=sasl_server_module_name,
               MODULE_LIBDEPS_MONGOS=sasl_server_module_name)

    distsrc = env.Dir(root).Dir('distsrc')
    docs = env.Dir(root).Dir('docs')
    env.Append(MODULE_BANNERS=[
            distsrc.File('LICENSE.txt'),
            docs.File('MONGO-MIB.txt'),
            docs.File('mongod.conf'),
            docs.File('snmp.md')
            ])
