import os

def configure(conf, env):
    root = os.path.dirname(__file__)

    env.Append(CPPDEFINES=dict(MONGO_ENTERPRISE_VERSION=1))

    if not conf.CheckCXXHeader("net-snmp/net-snmp-config.h"):
        print("Could not find <net-snmp/net-snmp-config.h>, required for enterprise build.")
        env.Exit(1)

    if env['PYSYSPLATFORM'] == "win32":
        env['SNMP_SYSLIBDEPS'] = ['netsnmp','netsnmpagent','netsnmpmibs']
        env.Append(CPPDEFINES=["NETSNMP_NO_INLINE"])
    else:
        try:
            snmpFlags = env.ParseFlags("!net-snmp-config --agent-libs")
        except OSError, ose:
            # the net-snmp-config command was not found
            print( "Could not find or execute 'net-snmp-config'" )
            print( ose )
            env.Exit(1)
        else:
            env['SNMP_SYSLIBDEPS'] = snmpFlags['LIBS']
            del snmpFlags['LIBS']
            env.Append(**snmpFlags)
            env.Append(CPPDEFINES=["NETSNMP_NO_INLINE"])

    env['MONGO_BUILD_SASL_CLIENT'] = True
    if not conf.CheckLibWithHeader(
        "sasl2",
        ["stddef.h","sasl/sasl.h"], "C",
        "sasl_version_info(0, 0, 0, 0, 0, 0);",
        autoadd=False):

        print("Could not find <sasl/sasl.h> and sasl library, required for enterprise build.")
        env.Exit(1)

    if conf.CheckLib(library="gssapi_krb5", autoadd=False):
        env['MONGO_GSSAPI_IMPL'] = "gssapi"
        env['MONGO_GSSAPI_LIB'] = "gssapi_krb5"
    elif env['PYSYSPLATFORM'] == "win32":
        env['MONGO_GSSAPI_IMPL'] = "sspi"
        env['MONGO_GSSAPI_LIB'] = "secur32"
    else:
        print("Could not find gssapi_krb5 library nor Windows OS, required for enterprise build.")
        env.Exit(1)

    distsrc = env.Dir(root).Dir('distsrc')
    docs = env.Dir(root).Dir('docs')
    env.Append(MODULE_BANNERS=[
            distsrc.File('LICENSE.txt'),
            docs.File('MONGO-MIB.txt'),
            docs.File('mongod.conf.master'),
            docs.File('mongod.conf.subagent'),
            docs.File('README-snmp.txt')
            ])
