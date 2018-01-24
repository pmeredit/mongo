// Verify that setParameter authFailedDelayMs is "settable" and "gettable"

function setAndCheckParameter(dbConn, parameterName, newValue, expectedResult) {
    jsTest.log("Test setting parameter: " + parameterName + " to value: " + newValue);
    var getParameterCommand = {getParameter: 1};
    getParameterCommand[parameterName] = 1;
    var ret = dbConn.adminCommand(getParameterCommand);
    assert.eq(ret.ok, 1, tojson(ret));
    oldValue = ret[parameterName];

    var setParameterCommand = {setParameter: 1};
    setParameterCommand[parameterName] = newValue;
    var ret = dbConn.adminCommand(setParameterCommand);
    assert.eq(ret.ok, 1, tojson(ret));
    assert.eq(ret.was, oldValue, tojson(ret));

    var ret = dbConn.adminCommand(getParameterCommand);
    assert.eq(ret.ok, 1, tojson(ret));
    // If we have explicitly set an "exptectedResult", use that, else use "newValue".  This is for
    // cases where the server does some type coersion that changes the value.
    if (typeof expectedResult === "undefined") {
        assert.eq(ret[parameterName], newValue, tojson(ret));
    } else {
        assert.eq(ret[parameterName], expectedResult, tojson(ret));
    }
    return newValue;
}

function ensureSetParameterFailure(dbConn, parameterName, newValue) {
    jsTest.log("Test setting parameter: " + parameterName + " to invalid value: " + newValue);
    var setParameterCommand = {setParameter: 1};
    setParameterCommand[parameterName] = newValue;
    var ret = dbConn.adminCommand(setParameterCommand);
    assert.eq(ret.ok, 0, tojson(ret));
    printjson(ret);
}

var dbConn = MongoRunner.runMongod({setParameter: 'authFailedDelayMs=100'});
setAndCheckParameter(dbConn, "authFailedDelayMs", 1000);
ensureSetParameterFailure(dbConn, "authFailedDelayMs", 10000);
MongoRunner.stopMongod(dbConn);