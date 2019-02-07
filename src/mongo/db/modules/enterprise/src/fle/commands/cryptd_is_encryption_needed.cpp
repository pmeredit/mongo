/**
 * Copyright (C) 2019 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/db/commands.h"

#include "../markings/markings.h"
#include "fle/commands/cryptd_commands_gen.h"

namespace mongo {
namespace {

/**
 * Implements { isEncryptionNeeded : 1} for mongocryptd
 *
 * NOTE: The only method called is run(), the rest exist simply to ensure the code compiles.
 */
class CryptDIsEncryptionNeededCommand final : public TypedCommand<CryptDIsEncryptionNeededCommand> {
public:
    using Request = IsEncryptionNeeded;

    std::string help() const final {
        return "Check if the JSON schema has encryption";
    }

    bool adminOnly() const final {
        return false;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const final {
        return AllowedOnSecondary::kNever;
    }

    class Invocation final : public InvocationBase {
    public:
        using InvocationBase::InvocationBase;

        IsEncryptionNeededResponse typedRun(OperationContext* opCtx) {
            IsEncryptionNeededResponse resp;
            resp.setRequiresEncryption(isEncryptionNeeded(request().getJsonSchema()));
            return resp;
        }

    private:
        NamespaceString ns() const final {
            return NamespaceString(request().getDbName());
        }

        bool supportsWriteConcern() const final {
            return false;
        }

        void doCheckAuthorization(OperationContext* opCtx) const final {}
    };

} cryptDIsEncryptionNeededCommand;

}  // namespace
}  // namespace mongo
