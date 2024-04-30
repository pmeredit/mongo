/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/commands.h"
#include "mongo/db/server_feature_flags_gen.h"
#include "streams/commands/stream_ops_gen.h"
#include "streams/management/stream_manager.h"

namespace streams {

using namespace mongo;

/**
 * streams_testOnlyInsert command implementation.
 */
class InsertCmd : public TypedCommand<InsertCmd> {
public:
    using Request = TestOnlyInsertCommand;
    using Reply = TestOnlyInsertCommand::Reply;

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }
    bool adminOnly() const override {
        return false;
    }
    std::string help() const override {
        return "Inserts the given documents into a stream.";
    }
    bool requiresAuth() const override {
        return false;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;
        void typedRun(OperationContext* opCtx) {
            TestOnlyInsertCommand requestParams = request();
            StreamManager* streamManager = getStreamManager(opCtx->getServiceContext());

            // The incoming documents may not be owned. Since we need them to outlive this command
            // execution, get owned copies of them.
            std::vector<mongo::BSONObj> ownedDocs;
            for (const auto& doc : requestParams.getDocuments()) {
                ownedDocs.push_back(doc.getOwned());
            }
            streamManager->testOnlyInsertDocuments(requestParams.getName().toString(),
                                                   std::move(ownedDocs));
        }

    private:
        bool supportsWriteConcern() const final {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {
            // We have no authorization check for now.
        }

        NamespaceString ns() const final {
            return NamespaceString(request().getDbName());
        }
    };
};

MONGO_REGISTER_COMMAND(InsertCmd).requiresFeatureFlag(&mongo::gFeatureFlagStreams).forShard();

}  // namespace streams
