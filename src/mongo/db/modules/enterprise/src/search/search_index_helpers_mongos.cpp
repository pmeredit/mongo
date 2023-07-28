/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "search/search_index_helpers_mongos.h"

#include "mongo/db/list_collections_gen.h"
#include "mongo/db/service_context.h"
#include "mongo/s/cluster_commands_helpers.h"
#include "mongo/s/grid.h"
#include "mongo/s/router_role.h"

namespace mongo {

ServiceContext::ConstructorActionRegisterer searchIndexHelpersMongosImplementation{
    "searchIndexHelpersMongos-registration", [](ServiceContext* serviceContext) {
        invariant(serviceContext);
        SearchIndexHelpers::set(serviceContext, std::make_unique<SearchIndexHelpersMongos>());
    }};

UUID SearchIndexHelpersMongos::fetchCollectionUUIDOrThrow(OperationContext* opCtx,
                                                          const NamespaceString& nss) {

    // We perform a listCollection request to get the UUID from the actual primary shard for the
    // database. This will ensure it is correct for both SHARDED and UNSHARDED versions of the
    // collection.

    sharding::router::DBPrimaryRouter router{opCtx->getServiceContext(), nss.db_deprecated()};

    auto uuid = router.route(
        opCtx,
        "get collection UUID",
        [&](OperationContext* opCtx, const CachedDatabaseInfo& cdb) -> boost::optional<UUID> {
            ListCollections listCollections;
            listCollections.setDbName(nss.dbName());
            listCollections.setFilter(BSON("name" << nss.coll()));

            auto response = executeCommandAgainstDatabasePrimary(
                opCtx,
                nss.db_deprecated(),
                cdb,
                listCollections.toBSON({}),
                ReadPreferenceSetting(ReadPreference::PrimaryPreferred),
                Shard::RetryPolicy::kIdempotent);

            // We consider an empty response to mean that the collection doesn't exist.
            auto batch = uassertStatusOK(response.swResponse).data["cursor"]["firstBatch"].Array();
            if (batch.empty()) {
                return boost::none;
            }
            const auto& bsonDoc = batch.front();
            auto uuid = UUID::parse(bsonDoc["info"]["uuid"]);
            if (!uuid.isOK()) {
                return boost::none;
            }
            return uuid.getValue();
        });
    if (!uuid) {
        uasserted(ErrorCodes::NamespaceNotFound,
                  str::stream() << "collection " << nss.toStringForErrorMsg() << " does not exist");
    }
    return *uuid;
}

}  // namespace mongo
