# -*- mode: python -*-

Import("env usev8 usesm")

env.StaticLibrary('mongosnmp',
                  ['src/snmp.cpp',
                   'src/snmp_oid.cpp'],
                  SYSLIBDEPS=env.get('SNMP_SYSLIBDEPS', []))

env.StaticLibrary('mongosaslservercommon',
                  ['src/sasl_authentication_session.cpp',
                   'src/sasl_commands.cpp',
                   'src/gcrypt_init.cpp',
                   ],
                  SYSLIBDEPS=['dl', 'gsasl'])

mongosaslshell_files = ['src/sasl_shell.cpp']
if usesm:
    mongosaslshell_files.extend(['src/sasl_shell_authenticate_sm.cpp'])
if usev8:
    mongosaslshell_files.extend(['src/sasl_shell_authenticate_v8.cpp'])

env.StaticLibrary('mongosaslshell', mongosaslshell_files)
