// Check for structured log data during LDAP queries.

(function() {
'use strict';

const ENTERPRISE = 'src/mongo/db/modules/enterprise';
load(ENTERPRISE + '/jstests/external_auth/lib/ldap_authz_lib.js');

const authOptions = {
    user: 'ldapz_ldap1',
    pwd: defaultPwd,
    mechanism: "PLAIN",
    digestPassword: false
};

const configGenerator = new LDAPTestConfigGenerator();
configGenerator.useLogFiles = false;
configGenerator.ldapAuthzQueryTemplate =
    'ou=Groups,dc=10gen,dc=cc??one?(&(objectClass=groupOfNames)(member={USER}))';
configGenerator.ldapUserToDNMapping = [
    {match: '(.+)', ldapQuery: 'cn={0},' + defaultUserDNSuffix},
];

function countContexts(opts) {
    authAndVerify(opts);

    jsTest.log('Checking log for LDAPQuery entries');

    // Get admin DB on appropriate node.
    const admin = (function() {
                      if (opts.shardingTest) {
                          return opts.shardingTest.c0;
                      } else if (opts.replSetTest) {
                          return opts.replSetTest.getPrimary();
                      } else {
                          return opts.conn;
                      }
                  })().getDB('admin');

    // Need to auth as admin to get log.
    assert(admin.auth('siteRootAdmin', 'secret'));
    const log = assert.commandWorked(admin.runCommand({getLog: 'global'})).log;
    const log4615666 = log.map((l) => JSON.parse(l)).filter((l) => l.id === 4615666);
    const contextCounts = {
        livenessCheck: 0,
        userToDNMapping: 0,
        queryTemplate: 0,
    };
    log4615666.forEach(function(line) {
        assert(line.attr !== undefined);
        assert(line.attr.query !== undefined);
        ++contextCounts[String(line.attr.query.context)];
    });
    jsTest.log(tojson(contextCounts));

    // We expect only the above known context types,
    // and for all three to have a count >= 1.
    const contextCountKeys = Object.keys(contextCounts);
    assert.eq(contextCountKeys.length, 3, "Unexpected query context");
    contextCountKeys.forEach(function(key) {
        const count = contextCounts[key];
        assert.gte(count, 1, `Expected LDAP query type ${key}`);
    });

    admin.logout();
}

runTests(countContexts, configGenerator, {authOptions: authOptions, user: authOptions.user});
})();
