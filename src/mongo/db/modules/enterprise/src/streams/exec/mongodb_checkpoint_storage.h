#pragma once

#include "mongo/db/service_context.h"
#include "mongo/idl/idl_parser.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/context.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/old_checkpoint_storage.h"

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
 * This class is not thread safe. There is an instance of MongoDBCheckpointStorage
 * per streamProcessor.
 *
 * In SERVER-78634 and SERVER-78634 we will make improvements to this implementation
 * to deal with large amounts of state.
 * In SERVER-75959 we will add retry logic to this class for transient connection failures.
 */
class MongoDBCheckpointStorage : public OldCheckpointStorage {
public:
    struct Options {
        // Service context used to retrieve mongocxx::instance.
        mongo::ServiceContext* svcCtx;
        // Auth, network, and db/collection name options to connect to a MongoDB.
        MongoCxxClientOptions mongoClientOptions;
    };

    MongoDBCheckpointStorage(Context* context, Options options);

protected:
    CheckpointId doCreateCheckpointId() override;

    void doAddState(CheckpointId checkpointId,
                    OperatorId operatorId,
                    mongo::BSONObj operatorState,
                    int32_t chunkNumber) override;

    void doCommit(CheckpointId checkpointId, mongo::CheckpointInfo checkpointInfo) override;

    boost::optional<CheckpointId> doReadLatestCheckpointId() override;

    boost::optional<mongo::BSONObj> doReadState(CheckpointId checkpointId,
                                                OperatorId operatorId,
                                                int32_t chunkNumber) override;

    boost::optional<mongo::CheckpointInfo> doReadCheckpointInfo(CheckpointId checkpointId) override;

private:
    std::string getOperatorStateDocId(CheckpointId checkpointId,
                                      OperatorId operatorId,
                                      int32_t chunkNumber);
    CheckpointId fromCheckpointDocId(const std::string& checkpointIdStr);

    // Options for this class.
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
