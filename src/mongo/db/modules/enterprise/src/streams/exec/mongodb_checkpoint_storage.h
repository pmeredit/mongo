#pragma once

#include "mongo/db/service_context.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/checkpoint_storage.h"

#include <boost/optional/optional.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

namespace mongo {
class ServiceContext;
}

namespace streams {

/**
 * This is an initial implementation of checkpoint storage
 * using MongoDB. It uses the mongocxx driver to interact with
 * a remote MongoDB.
 *
 * This class is not thread safe. There is an instance of CheckpointMongoDBStorage
 * per streamProcessor.
 *
 * In SERVER-78634 and SERVER-78634 we will make improvements to this implementation
 * to deal with large amounts of state.
 * In SERVER-75959 we will add retry logic to this class for transient connection failures.
 */
class MongoDBCheckpointStorage : public CheckpointStorage {
public:
    struct Options {
        // Tenant ID that this stream processor belongs to.
        std::string tenantId;
        // Logical identifier for this stream processor.
        // Guaranteed by the service components to remain the same
        // across starts for the same stream processor definition.
        std::string streamProcessorId;
        // Service context used to retrieve mongocxx::instance.
        mongo::ServiceContext* svcCtx;
        // MongoDB URI used for checkpoint data.
        std::string mongodbUri;
        // Database for checkpoint data.
        std::string database;
        // Collection for checkpoint data.
        std::string collection;
    };

    MongoDBCheckpointStorage(Options options);

protected:
    CheckpointId doCreateCheckpointId() override;

    void doAddState(CheckpointId checkpointId,
                    OperatorId operatorId,
                    mongo::BSONObj operatorState,
                    int32_t chunkNumber) override;

    void doCommit(CheckpointId checkpointId) override;

    boost::optional<CheckpointId> doReadLatestCheckpointId() override;

    boost::optional<mongo::BSONObj> doReadState(CheckpointId checkpointId,
                                                OperatorId operatorId,
                                                int32_t chunkNumber) override;

private:
    std::string getOperatorStateDocId(CheckpointId checkpointId,
                                      OperatorId operatorId,
                                      int32_t chunkNumber);
    std::string getCheckpointDocId(const std::string& prefix, CheckpointId checkpointId);
    CheckpointId fromCheckpointDocId(const std::string& checkpointIdStr);

    Options _options;
    mongo::IDLParserContext _parserContext;
    // Insert options for writing OperatorState and CheckpointInfo documents.
    mongocxx::options::insert _insertOptions;
    // Prefix used for querying CheckpointInfo documents for this tenantId and streamProcessorId.
    const std::string _checkpointDocIdPrefix;
    // Prefix used for querying OperatorState documents for this tenantId and streamProcessorId.
    const std::string _operatorDocIdPrefix;
    // Members for mongocxx client.
    mongocxx::instance* _instance{nullptr};
    std::unique_ptr<mongocxx::uri> _uri;
    std::unique_ptr<mongocxx::client> _client;
    std::unique_ptr<mongocxx::database> _database;
    std::unique_ptr<mongocxx::collection> _collection;
};

}  // namespace streams
