// Some basic tests of Cyrus SASL mechanisms

(function() {
    var m = MongoRunner.runMongod({
        auth: "",
        bind_ip: "127.0.0.1",
        useHostname: false,
        setParameter: "authenticationMechanisms=PLAIN,SCRAM-SHA-1,CRAM-MD5"
    });
    var db = m.getDB("admin");

    db.createUser({
        user: "testUser",
        pwd: "Pswd",
        roles: [
            {role: "userAdminAnyDatabase", db: "admin"},
            {role: "readWriteAnyDatabase", db: "admin"}
        ]
    });

    // It should not be possible to log into the database without a password
    assert(!db.auth({user: "testUser", pwd: "", mechanism: "SCRAM-SHA-1"}));
    assert(!db.auth({user: "testUser", pwd: "", mechanism: "PLAIN", digestPassword: false}));
    assert(!db.auth({user: "testUser", pwd: "", mechanism: "CRAM-MD5", digestPassword: false}));

    // It should not be possible to log into the database with the wrong password
    assert(!db.auth({user: "testUser", pwd: "wrong", mechanism: "SCRAM-SHA-1"}));
    assert(!db.auth({user: "testUser", pwd: "wrong", mechanism: "PLAIN"}));
    assert(!db.auth({user: "testUser", pwd: "wrong", mechanism: "CRAM-MD5"}));

    // It should be possible to log into the database with the correct password
    // Non-SCRAM mechanisms have been disabled pending SERVER-16668
    assert(db.auth({user: "testUser", pwd: "Pswd", mechanism: "SCRAM-SHA-1"}));
    // assert(db.auth({user: "testUser", pwd: "Pswd", mechanism: "PLAIN"}));
    // assert(db.auth({user: "testUser", pwd: "Pswd", mechanism: "CRAM-MD5"}));
})();
