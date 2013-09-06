# -*- mode: python -*-

Import("env")

env.StaticLibrary('audit',
                  ['src/audit/audit.cpp',
                   'src/audit/audit_authentication.cpp',
                   'src/audit/audit_authz_check.cpp',
                   'src/audit/audit_event.cpp',
                   'src/audit/audit_log_domain.cpp',
                   'src/audit/audit_private.cpp',
                   ],
                  LIBDEPS=['$BUILD_DIR/mongo/base/base',
                           '$BUILD_DIR/mongo/logger/logger'],
                  LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/${LIBPREFIX}coredb${LIBSUFFIX}'])

env.StaticLibrary('mongosnmp',
                  ['src/snmp.cpp',
                   'src/snmp_oid.cpp'],
                  LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/${PROGPREFIX}mongod${PROGSUFFIX}'],
                  SYSLIBDEPS=env.get('SNMP_SYSLIBDEPS', []))

env.StaticLibrary('mongosaslserversession',
                  ['src/auxprop_mongodb_internal.cpp',
                   'src/canon_mongodb_internal.cpp',
                   'src/mongo_${MONGO_GSSAPI_IMPL}.cpp',
                   'src/sasl_authentication_session.cpp',
                   ],
                  LIBDEPS=['$BUILD_DIR/mongo/server_parameters',
                           '$BUILD_DIR/mongo/db/auth/authmocks'],
                  SYSLIBDEPS=['sasl2', '${MONGO_GSSAPI_LIB}'])

env.StaticLibrary('mongosaslservercommon',
                  ['src/sasl_commands.cpp'],
                  LIBDEPS=['mongosaslserversession'],
                  LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/${PROGPREFIX}mongod${PROGSUFFIX}',
                                      '$BUILD_DIR/mongo/${PROGPREFIX}mongos${PROGSUFFIX}'])

env.CppUnitTest('sasl_authentication_session_test',
                ['src/sasl_authentication_session_test.cpp'],
                LIBDEPS=['mongosaslserversession',
                         '$BUILD_DIR/mongo/bson',
                         '$BUILD_DIR/mongo/db/auth/authcore',
                         '$BUILD_DIR/mongo/sasl_client_session'])

if env['PYSYSPLATFORM'] == "win32":
    sspi_test = env.Program('sasl_authentication_session_sspi_test',
                            ['src/sasl_authentication_session_sspi_test.cpp'],
                            LIBDEPS=['mongosaslserversession'])
# SERVER-10700
#    env.RegisterUnitTest(sspi_test[0])
else:
    gssapi_test = env.Program('sasl_authentication_session_gssapi_test',
                              ['src/sasl_authentication_session_gssapi_test.cpp'],
                              LIBDEPS=['mongosaslserversession',
                                       '$BUILD_DIR/mongo/bson',
                                       '$BUILD_DIR/mongo/db/auth/authcore',
                                       '$BUILD_DIR/mongo/sasl_client_session',
                                       '$BUILD_DIR/mongo/unittest/unittest',
                                       '$BUILD_DIR/mongo/unittest/unittest_crutch'])
    env.RegisterUnitTest(gssapi_test[0])
