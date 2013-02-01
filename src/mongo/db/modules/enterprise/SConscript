# -*- mode: python -*-

Import("env")

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
