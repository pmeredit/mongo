from __future__ import print_function

import os
import os.path
import SCons.Script.Main

def configure(conf, env):
    root = os.path.dirname(__file__)

    SCons.Script.Main.AddOption("--enterprise-features",
        default='*',
        dest='enterprise_features',
        action='store',
        type='string',
        const='*',
        nargs='?',
    )

    enterprise_features_option = env.GetOption('enterprise_features')

    all_features = [
        "audit",
        "encryptdb",
        "hot_backups",
        "inmemory",
        "ldap",
        "queryable",
        "sasl",
        "snmp",
        "watchdog",
    ]

    selected_features = []
    configured_modules = []

    env['MONGO_ENTERPRISE_VERSION'] = True
    if enterprise_features_option == '*':
        selected_features = all_features
        configured_modules.append("enterprise")
    else:
        for choice in enterprise_features_option.split(','):
            if choice in all_features and not choice in selected_features:
                selected_features.append(choice)
            else:
                env.ConfError("Bad --enterprise-feature choice '{}'; choose from {} or '*'".format(choice, ", ".join(all_features)))

        if 'watchdog' in selected_features and not 'audit' in selected_features:
            env.ConfError("the watchdog enterprise feature depends on the audit enterprise feature")
        if 'sasl' in selected_features and not 'ldap' in selected_features:
            env.ConfError("the sasl enterprise feature depends on the ldap enterprise feature")
        if 'snmp' in selected_features and not 'watchdog' in selected_features:
            env.ConfError("the snmp enterprise feature depends on the watchdog enterprise feature")

        configured_modules.extend(selected_features)

    env['MONGO_ENTERPRISE_FEATURES'] = selected_features

    if 'sasl' in env['MONGO_ENTERPRISE_FEATURES']:
        env['MONGO_BUILD_SASL_CLIENT'] = True

    if 'snmp' in env['MONGO_ENTERPRISE_FEATURES']:

        if not env.TargetOSIs('darwin'):
            if not conf.CheckCXXHeader("net-snmp/net-snmp-config.h"):
                env.ConfError("Could not find <net-snmp/net-snmp-config.h>, required for "
                              "enterprise build.")

        snmpFlags = None
        if not env.TargetOSIs("windows", "darwin"):
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
                #
                # We need to do LINKFLAGS globally since it affects runtime of executables.
                env.Append(LINKFLAGS=[flag for flag in snmpFlags['LINKFLAGS'] if '-Wl,-rpath' in flag])


    # Compute a path to use for including files that are generated in the build directory.
    # Computes: $BUILD_DIR/mongo/db/modules/<enterprise_module_name>/src
    src_include_path = os.path.join("$BUILD_DIR", str(env.Dir(root).Dir('src'))[4:] )

    def injectEnterpriseModule(env, consumer=True, builder=False):
        # Inject an include path so that idl generated files can be included
        env.Append(CPPPATH=[src_include_path])

        if consumer:
            if 'MONGO_ENTERPRISE_VERSION' in env:
                env.Append(CPPDEFINES=[("MONGO_ENTERPRISE_VERSION", 1)])
            if 'audit' in env['MONGO_ENTERPRISE_FEATURES']:
                env.Append(CPPDEFINES=[("MONGO_ENTERPRISE_AUDIT", 1)])
            if 'encryptdb' in env['MONGO_ENTERPRISE_FEATURES']:
                env.Append(CPPDEFINES=[("MONGO_ENTERPRISE_ENCRYPTDB", 1)])

        if builder:
            if 'snmp' in env['MONGO_ENTERPRISE_FEATURES']:
                if env.TargetOSIs("windows"):
                    env['SNMP_SYSLIBDEPS'] = ['netsnmp','netsnmpagent','netsnmpmibs']
                    env.Append(CPPDEFINES=["NETSNMP_NO_INLINE"])
                elif not env.TargetOSIs("darwin"):
                    env['SNMP_SYSLIBDEPS'] = snmpFlags['LIBS']
                    env.Append(LIBPATH=snmpFlags['LIBPATH'])
                    env.Append(CPPDEFINES=["NETSNMP_NO_INLINE"])
        return env

    env['MODULE_INJECTORS']['enterprise'] = injectEnterpriseModule

    distsrc = env.Dir(root).Dir('distsrc')
    docs = env.Dir(root).Dir('docs')
    env.Append(MODULE_BANNERS=[
            distsrc.File('LICENSE-Enterprise.txt'),
            ])
    if env.TargetOSIs("windows"):
        env.Append(MODULE_BANNERS=[
            distsrc.File('THIRD-PARTY-NOTICES.windows'),
        ])

    if not env.TargetOSIs("darwin"):
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

    if 'encryptdb' in env['MONGO_ENTERPRISE_FEATURES']:
        env.Append(DIST_BINARIES=[ "enterprise/mongodecrypt" ])
    if 'ldap' in env['MONGO_ENTERPRISE_FEATURES']:
        env.Append(DIST_BINARIES=[ "enterprise/mongoldap" ])

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
        import re
        import subprocess
        import _winreg

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

        # This magic on how to locate vcredist_x64.exe is taken from src/mongo/installer/msi/SConscript in the community repo
        programfilesx86 = os.environ.get('ProgramFiles(x86)')
        if programfilesx86 is None:
            programfilesx86 = "C:\\Program Files (x86)"
        vsinstall_path = subprocess.check_output([os.path.join(programfilesx86, "Microsoft Visual Studio", "Installer", "vswhere.exe"), "-version", "[15.0,16.0)", "-property", "installationPath", "-nologo"]).strip()
        vsruntime_key = _winreg.OpenKey(_winreg.HKEY_LOCAL_MACHINE, "SOFTWARE\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\x64")
        vslib_version,vslib_version_type = _winreg.QueryValueEx(vsruntime_key, "Version")

        # Get library version from registry and fallback to directory search if not found as expected on disk
        redist_root = os.path.join(vsinstall_path, "VC", "Redist", "MSVC")
        redist_path = os.path.join(redist_root, re.match("v(\d+\.\d+\.\d+)\.\d+", vslib_version).group(1))
        if not os.path.isdir(redist_path):
            dirs = os.listdir(redist_root)
            dirs.sort()
            for dir in reversed(dirs):
                candidate = os.path.join(redist_root, dir)
                if os.path.isdir(candidate):
                    redist_path = candidate
                    break

        redist_file_name = os.path.join(redist_path, "vcredist_x64.exe")
        env.Append(ARCHIVE_ADDITIONS=[redist_file_name])
        env.Append(ARCHIVE_ADDITION_DIR_MAP={
                redist_path : "bin"
                })

    return configured_modules
