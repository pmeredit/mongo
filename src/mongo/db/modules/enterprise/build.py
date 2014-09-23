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
            ])
    env.Append(ARCHIVE_ADDITIONS=[
            docs.File('MONGOD-MIB.txt'),
            docs.File('MONGODBINC-MIB.txt'),
            docs.File('mongod.conf.master'),
            docs.File('mongod.conf.subagent'),
            docs.File('README-snmp.txt')
            ])
    env.Append(ARCHIVE_ADDITION_DIR_MAP={
            str(env.Dir(root).Dir('docs')): "snmp"
            })

    def addFileInExtraPath(file_name):
        paths = env['EXTRABINPATH']
        for path in paths:
            full_file_name = os.path.join(os.path.normpath(path.lower()), file_name)
            if os.path.exists(full_file_name):
                env.Append(ARCHIVE_ADDITIONS=[full_file_name])

                env.Append(ARCHIVE_ADDITION_DIR_MAP={
                        os.path.normpath(path.lower()): "bin"
                        })
                break

    if env['PYSYSPLATFORM'] == "win32":
        files = [
                'libsasl.dll',
                'libsasl.pdb',
                'ssleay32.dll',
                'libeay32.dll',
                'netsnmp.dll',
                'netsnmp.pdb',
                ]

        for extra_file in files:
            addFileInExtraPath(extra_file)

        for path in env['ENV']['PATH'].split(";"):
            full_path = os.path.normpath(os.path.join(path, r"..\..\redist\1033")).lower()
            full_file_name = os.path.join(full_path, "vcredist_x64.exe")
            if os.path.exists(full_file_name):
                env.Append(ARCHIVE_ADDITIONS=[full_file_name])

                env.Append(ARCHIVE_ADDITION_DIR_MAP={
                        full_path : "bin"
                        })
                break

