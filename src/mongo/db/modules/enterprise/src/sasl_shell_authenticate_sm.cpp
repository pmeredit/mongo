/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include "mongo/pch.h"

#include <gsasl.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/scripting/engine_spidermonkey_internal.h"
#include "mongo/util/mongoutils/str.h"
#include "sasl_shell.h"

namespace mongo  {
namespace spidermonkey {
namespace {

    JSBool mongo_sasl_auth(JSContext* cx, JSObject* obj, uintN argc, jsval *argv, jsval* rval) {
        smuassert(cx, "mongo_sasl_auth expects one parameter", argc == 1);
        DBClientWithCommands *conn = getConnection(cx, obj);
        smuassert(cx , "no connection!", conn);
        Convertor c(cx);
        BSONObj saslParameters;
        try {
            saslParameters = c.toObject(argv[0]);
        }
        catch (MsgAssertionException& e) {
            if (!JS_IsExceptionPending(cx))
                JS_ReportError(cx, "%s", e.what());
            return JS_FALSE;
        }
        Status status = saslClientAuthenticate(
                mongo::getShellGsaslContext(), conn, saslParameters, NULL);
        if (!status.isOK()) {
            if (!JS_IsExceptionPending(cx))
                JS_ReportError(cx, "%s", std::string(str::stream() << status.codeString() << ": " <<
                                                     status.reason()).c_str());
            return JS_FALSE;
        }
        return JS_TRUE;
    }

    MONGO_INITIALIZER_GENERAL(
            SmAddSaslAuth, ("SmMongoFunctionsRegistry"), ("SmMongoFunctionRegistrationDone"))(
                    InitializerContext* context) {

        static const JSFunctionSpec saslFunctionSpec =
            { "saslAuthenticate", mongo_sasl_auth, 0, JSPROP_READONLY | JSPROP_PERMANENT, 0 };
        registerMongoFunction(saslFunctionSpec);
        return Status::OK();
    }

}  // namespace
}  // namespace spidermonkey
}  // namespace mongo
