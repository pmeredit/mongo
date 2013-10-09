# -*- mode: python -*-

Import("env")

env.StaticLibrary('audit',
                  ['src/audit/audit.cpp',
                   'src/audit/audit_authentication.cpp',
                   'src/audit/audit_authz_check.cpp',
                   'src/audit/audit_event.cpp',
                   'src/audit/audit_log_domain.cpp',
                   'src/audit/audit_manager_global.cpp',
                   'src/audit/audit_options.cpp',
                   'src/audit/audit_private.cpp',
                   ],
                  LIBDEPS=['$BUILD_DIR/mongo/base/base',
                           '$BUILD_DIR/mongo/logger/logger'],
                  LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/${LIBPREFIX}coredb${LIBSUFFIX}'])

env.StaticLibrary('mongosnmp',
                  ['src/snmp/serverstatus_client.cpp',
                   'src/snmp/snmp.cpp',
                   'src/snmp/snmp_oid.cpp'],
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

#env.CppUnitTest('sasl_authentication_session_test',
#                ['src/sasl_authentication_session_test.cpp'],
#                LIBDEPS=['mongosaslserversession',
#                         '$BUILD_DIR/mongo/bson',
#                         '$BUILD_DIR/mongo/db/auth/authcore',
#                         '$BUILD_DIR/mongo/sasl_client_session'])

if env['PYSYSPLATFORM'] == "win32":
    # Ensure we're building with /MD or /MDd
    mdFlags = ["/MD","/MDd"]
    hasFlag = 0
    for mdFlag in mdFlags:
        if mdFlag in env['CCFLAGS']:
            hasFlag += 1
    if hasFlag != 1:
        print("You must enable dynamic CRT --dynamic-windows to build the enterprise version")
        Exit(1)

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
