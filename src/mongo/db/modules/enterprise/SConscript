# -*- mode: python -*-

Import("env")

env.StaticLibrary('mongosnmp',
                  ['src/snmp.cpp',
                   'src/snmp_oid.cpp'],
                  SYSLIBDEPS=env.get('SNMP_SYSLIBDEPS', []))

env.StaticLibrary('mongosaslservercommon',
                  ['src/auxprop_mongodb_internal.cpp',
                   'src/canon_mongodb_internal.cpp',
                   'src/mongo_gssapi.cpp',
                   'src/sasl_authentication_session.cpp',
                   'src/sasl_commands.cpp',
                   ],
                  SYSLIBDEPS=['dl', 'sasl2', 'gssapi_krb5'])
