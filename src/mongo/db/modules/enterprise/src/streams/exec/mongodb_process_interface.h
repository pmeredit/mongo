#pragma once

#include <queue>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

#include "mongo/db/pipeline/process_interface/stub_mongo_process_interface.h"

namespace streams {

// An implementation of MongoProcessInterface that writes to the specified MongoDB instance.
class MongoDBProcessInterface : public mongo::StubMongoProcessInterface {
public:
    struct Options {
        mongo::ServiceContext* svcCtx;
        std::string mongodbUri;
        // Database and collection to write to.
        std::string database;
        std::string collection;
    };

    MongoDBProcessInterface(Options options);

    std::unique_ptr<WriteSizeEstimator> getWriteSizeEstimator(
        mongo::OperationContext* opCtx, const mongo::NamespaceString& ns) const override;

    mongo::Status insert(const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
                         const mongo::NamespaceString& ns,
                         std::vector<mongo::BSONObj>&& objs,
                         const mongo::WriteConcernOptions& wc,
                         boost::optional<mongo::OID> oid) override;

    mongo::StatusWith<UpdateResult> update(
        const boost::intrusive_ptr<mongo::ExpressionContext>& expCtx,
        const mongo::NamespaceString& ns,
        BatchedObjects&& batch,
        const mongo::WriteConcernOptions& wc,
        UpsertType upsert,
        bool multi,
        boost::optional<mongo::OID> oid) override;

private:
    Options _options;
    mongocxx::instance* _instance{nullptr};
    std::unique_ptr<mongocxx::uri> _uri;
    std::unique_ptr<mongocxx::client> _client;
    std::unique_ptr<mongocxx::database> _database;
    std::unique_ptr<mongocxx::collection> _collection;
};

}  // namespace streams
