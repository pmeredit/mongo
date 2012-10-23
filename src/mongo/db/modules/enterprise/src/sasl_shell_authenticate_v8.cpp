/*
 * Copyright (C) 2012 10gen, Inc.
 */

#include <v8.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/jsobj.h"
#include "mongo/scripting/engine_v8.h"
#include "mongo/scripting/v8_db.h"
#include "mongo/util/mongoutils/str.h"
#include "sasl_shell.h"

namespace {
    using namespace mongo;

    v8::Handle<v8::Value> mongoSaslAuth(V8Scope* scope, const v8::Arguments& args) {
        if (args.Length() != 1)
            return v8::ThrowException(v8::String::New("mongoSaslAuth expects one parameter"));

        DBClientWithCommands* conn = getConnection(args);
        if (NULL == conn)
            return v8::ThrowException(v8::String::New("no connection"));

        BSONObj saslParameters = scope->v8ToMongo(args[0]->ToObject());
        Status status = saslClientAuthenticate(
                getShellGsaslContext(), conn, saslParameters, NULL);
        if (!status.isOK()) {
            return v8::ThrowException(
                    v8::String::New(std::string(str::stream() << status.codeString() <<
                                                ": " << status.reason()).c_str()));
        }
        return v8::Undefined();
    }

    void addSaslAuthToMongoPrototype(V8Scope* scope,
                                     const v8::Handle<v8::FunctionTemplate>& mongo) {
        v8::Handle<v8::Template> proto = mongo->PrototypeTemplate();
        scope->injectV8Function("saslAuthenticate", mongoSaslAuth, proto);
    }

    MONGO_INITIALIZER_GENERAL(V8AddSaslAuth,
                              ("V8MongoPrototypeManipulatorRegistry"),
                              ("V8MongoPrototypeManipulatorRegistrationDone"))
        (InitializerContext* context) {

        v8RegisterMongoPrototypeManipulator(addSaslAuthToMongoPrototype);
        return Status::OK();
    }

}  // namespace
