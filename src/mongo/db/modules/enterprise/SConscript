# -*- mode: python -*-

Import("env")

env.StaticLibrary('mongosnmp',
                  ['src/snmp.cpp',
                   'src/snmp_oid.cpp'],
                  SYSLIBDEPS=env.get('SNMP_SYSLIBDEPS', []))
