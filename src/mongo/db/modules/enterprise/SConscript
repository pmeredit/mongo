# -*- mode: python -*-

Import("env")

env.Library('audit',
            ['src/audit/audit_application_message.cpp',
             'src/audit/audit_authentication.cpp',
             'src/audit/audit_authz_check.cpp',
             'src/audit/audit_command.cpp',
             'src/audit/audit_event.cpp',
             'src/audit/audit_indexes_collections_databases.cpp',
             'src/audit/audit_log_domain.cpp',
             'src/audit/audit_manager_global.cpp',
             'src/audit/audit_options.cpp',
             'src/audit/audit_private.cpp',
             'src/audit/audit_replset.cpp',
             'src/audit/audit_role_management.cpp',
             'src/audit/audit_sharding.cpp',
             'src/audit/audit_shutdown.cpp',
             'src/audit/audit_user_management.cpp',
             ],
            LIBDEPS=['$BUILD_DIR/mongo/base/base',
                     '$BUILD_DIR/mongo/logger/logger'],
            LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/${LIBPREFIX}coredb${LIBSUFFIX}'])

env.Library('audit_d',
            ['src/audit/impersonate_helpers_d.cpp'],
            LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/${LIBPREFIX}serveronly${LIBSUFFIX}'])

env.Library('audit_s',
            ['src/audit/impersonate_helpers_s.cpp'],
            LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/${LIBPREFIX}mongoscore${LIBSUFFIX}',
                                '$BUILD_DIR/mongo/${LIBPREFIX}coreshard${LIBSUFFIX}'])

# The auditing code needs to be built into the "coredb" library because there is code in there that
# references audit functions.  However, the "coredb" library is also currently shared by server
# programs, such as mongod and mongos, as well as client programs, such as mongodump and
# mongoexport.  For these reasons, we have no choice but to build the audit module into all of
# these, even though it's dead code in the client programs.  To avoid actually allowing the user to
# configure this dead code, we need to separate the option registration into this
# "audit_configuration" library and add it to only mongod and mongos.  Because audit defaults to
# disabled this effectively prevents this code from being run in client programs.
env.Library('audit_configuration',
            'src/audit/audit_options_init.cpp',
            LIBDEPS=['audit'],
            LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/${PROGPREFIX}mongod${PROGSUFFIX}',
                                '$BUILD_DIR/mongo/${PROGPREFIX}mongos${PROGSUFFIX}'])

env.Library('mongosnmp',
            ['src/snmp/serverstatus_client.cpp',
             'src/snmp/snmp.cpp',
             'src/snmp/snmp_oid.cpp',
             'src/snmp/snmp_options.cpp'
             ],
            LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/${PROGPREFIX}mongod${PROGSUFFIX}'],
            SYSLIBDEPS=env.get('SNMP_SYSLIBDEPS', []))

env.Library('mongosaslserversession',
            ['src/sasl/auxprop_mongodb_internal.cpp',
             'src/sasl/canon_mongodb_internal.cpp',
             'src/sasl/mongo_${MONGO_GSSAPI_IMPL}.cpp',
             'src/sasl/sasl_authentication_session.cpp',
             'src/sasl/sasl_options.cpp',
             ],
            LIBDEPS=['$BUILD_DIR/mongo/server_parameters',
                     '$BUILD_DIR/mongo/db/auth/authmocks'],
            SYSLIBDEPS=['sasl2', '${MONGO_GSSAPI_LIB}'])

env.Library('mongosaslservercommon',
            ['src/sasl/sasl_commands.cpp'],
            LIBDEPS=['mongosaslserversession'],
            LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/${PROGPREFIX}mongod${PROGSUFFIX}',
                                '$BUILD_DIR/mongo/${PROGPREFIX}mongos${PROGSUFFIX}'])

env.CppUnitTest('sasl_authentication_session_test',
                ['src/sasl/sasl_authentication_session_test.cpp'],
                LIBDEPS=['mongosaslserversession',
                         '$BUILD_DIR/mongo/bson',
                         '$BUILD_DIR/mongo/network',
                         '$BUILD_DIR/mongo/db/auth/authcore',
                         '$BUILD_DIR/mongo/sasl_client_session'])

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
                            ['src/sasl/sasl_authentication_session_sspi_test.cpp'],
                            LIBDEPS=['mongosaslserversession'])
# SERVER-10700
#    env.RegisterUnitTest(sspi_test[0])
else:
    gssapi_test = env.Program('sasl_authentication_session_gssapi_test',
                              ['src/sasl/sasl_authentication_session_gssapi_test.cpp'],
                              LIBDEPS=['mongosaslserversession',
                                       '$BUILD_DIR/mongo/bson',
                                       '$BUILD_DIR/mongo/db/auth/authcore',
                                       '$BUILD_DIR/mongo/sasl_client_session',
                                       '$BUILD_DIR/mongo/unittest/unittest',
                                       '$BUILD_DIR/mongo/unittest/unittest_crutch'])
    env.RegisterUnitTest(gssapi_test[0])
