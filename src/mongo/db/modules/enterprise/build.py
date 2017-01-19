import os
from SCons.Script import AddOption, GetOption

AddOption("--use-basis-tech-rosette-linguistics-platform",
          choices=["on", "off"],
          const="?",
          default="off",
          dest="rlp",
          help="Use Basis Tech Rosette Linguistics Platform",
          type="choice",
          )

def configure(conf, env):
    root = os.path.dirname(__file__)

    env.Append(CPPDEFINES=dict(MONGO_ENTERPRISE_VERSION=1))

    if not conf.CheckCXXHeader("net-snmp/net-snmp-config.h"):
        env.ConfError("Could not find <net-snmp/net-snmp-config.h>, required for "
            "enterprise build.")

    if env.TargetOSIs("windows"):
        env['SNMP_SYSLIBDEPS'] = ['netsnmp','netsnmpagent','netsnmpmibs']
        env.Append(CPPDEFINES=["NETSNMP_NO_INLINE"])
    else:
        try:
            snmpFlags = env.ParseFlags("!net-snmp-config --agent-libs")
        except OSError, ose:
            # the net-snmp-config command was not found
            env.ConfError("Could not find or execute 'net-snmp-config': {0}", ose)
        else:
            # Don't inject the whole environment held in snmpFlags, because the net-snmp-config
            # utility throws some weird flags in that get picked up in LINKFLAGS that we don't want
            # (see SERVER-23200). The only thing we need here are the LIBS and the LIBPATH, and any
            # rpath that was set in LINKFLAGS.
            env['SNMP_SYSLIBDEPS'] = snmpFlags['LIBS']
            env.Append(LIBPATH=snmpFlags['LIBPATH'])
            env.Append(LINKFLAGS=[flag for flag in snmpFlags['LINKFLAGS'] if '-Wl,-rpath' in flag])
            env.Append(CPPDEFINES=["NETSNMP_NO_INLINE"])

    env['MONGO_BUILD_SASL_CLIENT'] = True
    if not conf.CheckLibWithHeader(
        "sasl2",
        ["stddef.h","sasl/sasl.h"], "C",
        "sasl_version_info(0, 0, 0, 0, 0, 0);",
        autoadd=False):
        env.ConfError("Could not find <sasl/sasl.h> and sasl library, required for "
            "enterprise build.")

    if not env.TargetOSIs("windows") and not conf.CheckLibWithHeader(
        "curl",
        ["curl/curl.h"], "C",
        "curl_global_init(0);",
        autoadd=False):
        env.ConfError("Could not find <curl/curl.h> and curl lib, required for "
            "enterprise build")

    if not env.TargetOSIs("windows"):
        if not conf.CheckLibWithHeader(
                "ldap",
                ["ldap.h"], "C",
                "ldap_is_ldap_url(\"ldap://127.0.0.1\");", autoadd=False):
            env.ConfError("Could not find <ldap.h> and ldap library from OpenLDAP, "
                           "required for LDAP authorization in the enterprise build")
        if not conf.CheckLibWithHeader(
                "lber",
                ["lber.h"], "C",
                "ber_free(NULL, 0);", autoadd=False):
            env.ConfError("Could not find <lber.h> and lber library from OpenLDAP, "
                          "required for LDAP authorizaton in the enterprise build")
        env['MONGO_LDAP_LIB'] = ["ldap", "lber"]
    else:
        env['MONGO_LDAP_LIB'] = ["Wldap32"]

    if conf.CheckLib(library="gssapi_krb5", autoadd=False):
        env['MONGO_GSSAPI_IMPL'] = "gssapi"
        env['MONGO_GSSAPI_LIB'] = "gssapi_krb5"
        if not env.TargetOSIs('freebsd'):
            env['MONGO_GSSAPI_LIBDEPS'] = ['${MONGO_GSSAPI_LIB}']
        else:
            env['MONGO_GSSAPI_LIBDEPS'] = ['${MONGO_GSSAPI_LIB}', '${MONGO_GSSAPI_IMPL}']
    elif env.TargetOSIs("windows"):
        env['MONGO_GSSAPI_IMPL'] = "sspi"
        env['MONGO_GSSAPI_LIB'] = "secur32"
        env['MONGO_GSSAPI_LIBDEPS'] = ['${MONGO_GSSAPI_LIB}']
    else:
        env.ConfError("Could not find gssapi_krb5 library nor Windows OS, required for "
            "enterprise build.")

    distsrc = env.Dir(root).Dir('distsrc')
    docs = env.Dir(root).Dir('docs')
    env.Append(MODULE_BANNERS=[
            distsrc.File('LICENSE.txt'),
            ])
    if env.TargetOSIs("windows"):
        env.Append(MODULE_BANNERS=[
            distsrc.File('THIRD-PARTY-NOTICES.windows'),
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
    env.Append(DIST_BINARIES=[
        "enterprise/mongodecrypt",
        "enterprise/mongoldap"])

    def addFileInExtraPath(file_name):
        # We look for libraries in the peer 'bin' directory of each path in the libpath.
        paths = [str(env.Dir(libpath).Dir('..').Dir('bin')) for libpath in env['LIBPATH']]
        for path in paths:
            full_file_name = os.path.join(os.path.normpath(path.lower()), file_name)
            if os.path.exists(full_file_name):
                env.AppendUnique(ARCHIVE_ADDITIONS=[full_file_name])

                env.Append(ARCHIVE_ADDITION_DIR_MAP={
                        os.path.normpath(path.lower()): "bin"
                        })
                break

    if env.TargetOSIs("windows"):
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

    if GetOption("rlp") == "on":
        if not conf.CheckCXXHeader("bt_rlp_c.h"):
            env.ConfError("Could not find bt_rlp_c.h, include <BT_ROOT>/rlp/include in CPPPATH")
        if not conf.CheckCXXHeader("bt_types.h"):
            env.ConfError("Could not find bt_types.h, include <BT_ROOT>/rlp/utilities/include "
                "in CPPPATH")
