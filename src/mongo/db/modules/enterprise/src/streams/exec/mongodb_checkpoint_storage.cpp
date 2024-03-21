/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/mongodb_checkpoint_storage.h"

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/element.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/options/find.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/context.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/log_util.h"
#include "streams/exec/mongocxx_utils.h"

using namespace mongo;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::types::b_array;
using bsoncxx::types::b_regex;

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

namespace {

// Name of the primary _id field in checkpoint documents.
constexpr const char* kId = "_id";

// "state" field name in OperatorState.
constexpr const char* kState = "state";

// "checkpoint" prefix string for CheckpointInfo _id strings.
constexpr const char* kCheckpoint = "checkpoint";

// "operator" prefix string for OperatorState _id strings.
constexpr const char* kOperator = "operator";

bsoncxx::document::value makePrefixQuery(const std::string& field, const std::string& prefix) {
    return make_document(kvp(field, b_regex{fmt::format("^{}", prefix)}));
}

// The _id in the database looks like:
// checkpoint/{tenantId}/{streamProcessorId}/{unix timestamp}
std::string getFullCheckpointId(const std::string& prefix, CheckpointId checkpointId) {
    return fmt::format("{}{}", prefix, checkpointId);
}

}  // namespace

MongoDBCheckpointStorage::MongoDBCheckpointStorage(Context* context, Options options)
    : OldCheckpointStorage(context),
      _options(std::move(options)),
      _parserContext("MongoDBCheckpointStorage"),
      _checkpointDocIdPrefix(
          fmt::format("{}/{}/{}/", kCheckpoint, context->tenantId, context->streamProcessorId)),
      _operatorDocIdPrefix(
          fmt::format("{}/{}/{}/", kOperator, context->tenantId, context->streamProcessorId)) {
    _instance = getMongocxxInstance(_options.svcCtx);
    _uri = makeMongocxxUri(_options.mongoClientOptions.uri);
    _client = std::make_unique<mongocxx::client>(
        *_uri, _options.mongoClientOptions.toMongoCxxClientOptions());
    tassert(8143702, "Expected database name but got none", _options.mongoClientOptions.database);
    _database = std::make_unique<mongocxx::database>(
        _client->database(*_options.mongoClientOptions.database));
    tassert(
        8143703, "Expected collection name but got none", _options.mongoClientOptions.collection);
    _collection = std::make_unique<mongocxx::collection>(
        _database->collection(*_options.mongoClientOptions.collection));

    mongocxx::write_concern writeConcern;
    writeConcern.journal(true);
    writeConcern.acknowledge_level(mongocxx::write_concern::level::k_majority);
    // TODO(SERVER-75959): Handle timeouts, adjust this value.
    writeConcern.majority(/*timeout*/ stdx::chrono::milliseconds(60 * 1000));
    _insertOptions = mongocxx::options::insert().write_concern(std::move(writeConcern));
}

CheckpointId MongoDBCheckpointStorage::doCreateCheckpointId() {
    return Date_t::now().toMillisSinceEpoch();
}

void MongoDBCheckpointStorage::doAddState(CheckpointId checkpointId,
                                          OperatorId operatorId,
                                          BSONObj operatorState,
                                          int32_t chunkNumber) {
    // TODO(SERVER-78634): Write state in a background thread, and batch inserts whenever possible.
    std::string docId = getOperatorStateDocId(checkpointId, operatorId, chunkNumber);
    BSONObj state = OperatorState{std::move(docId), std::move(operatorState)}.toBSON();

    try {
        auto result = _collection->insert_one(toBsoncxxView(std::move(state)), _insertOptions);
        CHECKPOINT_WRITE_ASSERT(checkpointId, operatorId, "insert_one failure", result);
    } catch (mongocxx::exception& e) {
        LOGV2_ERROR(8112608,
                    "Failure while committing a checkpoint to storage.",
                    "exception"_attr = e.what());
        uasserted(8112609, "Failure while committing a checkpoint to storage.");
    }
}

boost::optional<BSONObj> MongoDBCheckpointStorage::doReadState(CheckpointId checkpointId,
                                                               OperatorId operatorId,
                                                               int32_t chunkNumber) {
    try {
        auto result = _collection->find_one(make_document(
            kvp(std::string{kId}, getOperatorStateDocId(checkpointId, operatorId, chunkNumber))));
        if (!result) {
            return boost::none;
        }
        return fromBsoncxxDocument(result->find(std::string{kState})->get_document());
    } catch (const mongocxx::exception& e) {
        LOGV2_ERROR(8112610,
                    "Failure while committing a checkpoint to storage.",
                    "exception"_attr = e.what());
        uasserted(8112611, "Failure while committing a checkpoint to storage.");
    }
}

void MongoDBCheckpointStorage::doCommit(CheckpointId checkpointId, CheckpointInfo checkpointInfo) {
    std::string fullCheckpointId = getFullCheckpointId(_checkpointDocIdPrefix, checkpointId);
    checkpointInfo.set_id(fullCheckpointId);
    try {
        auto result = _collection->insert_one(toBsoncxxView(std::move(checkpointInfo).toBSON()),
                                              _insertOptions);
        CHECKPOINT_WRITE_ASSERT(checkpointId, 0, "insert_one failure", result);
        LOGV2_INFO(74804,
                   "CheckpointStorage committed checkpoint",
                   "context"_attr = _context,
                   "fullCheckpointId"_attr = fullCheckpointId);
    } catch (const mongocxx::exception& e) {
        LOGV2_ERROR(8112602,
                    "Failure while committing a checkpoint to storage.",
                    "exception"_attr = e.what());
        uasserted(8112603, "Failure while committing a checkpoint to storage.");
    }
}

boost::optional<CheckpointId> MongoDBCheckpointStorage::doReadLatestCheckpointId() {
    const bsoncxx::document::value checkpointFindDoc(
        makePrefixQuery(std::string{kId}, _checkpointDocIdPrefix));
    const bsoncxx::document::value checkpointSortDoc(
        make_document(kvp(std::string{kId}, -1 /* descending */)));
    mongocxx::options::find checkpointFindOpts;
    checkpointFindOpts.sort(checkpointSortDoc.view());
    try {
        auto checkpointResult = _collection->find_one(checkpointFindDoc.view(), checkpointFindOpts);
        if (!checkpointResult) {
            return boost::none;
        }
        std::string id = fromBsoncxxDocument(*checkpointResult)[kId].str();
        return fromCheckpointDocId(std::move(id));
    } catch (const mongocxx::exception& e) {
        LOGV2_ERROR(
            8112604, "Failure while reading from checkpoint storage.", "exception"_attr = e.what());
        uasserted(8112605, "Failure while reading from checkpoint storage.");
    }
}

// The format is
// operator/{tenantId}/{streamProcessorId}/{checkpointId}/{operatorId}/{chunkNumber}
std::string MongoDBCheckpointStorage::getOperatorStateDocId(CheckpointId checkpointId,
                                                            OperatorId operatorId,
                                                            int32_t chunkNumber) {
    invariant(operatorId >= 0);
    // OperatorIds and chunkNumbers are 32 bit integers.
    // To support more efficient restores, we serialize these numbers as
    // 8 digit hex strings with {:08x}, so we can sort them.
    return fmt::format(
        "{}{}/{:08x}/{:08x}", _operatorDocIdPrefix, checkpointId, operatorId, chunkNumber);
}

// The format is
// checkpoint/{tenantId}/{streamProcessorId}/{checkpointId}
CheckpointId MongoDBCheckpointStorage::fromCheckpointDocId(const std::string& checkpointIdStr) {
    std::vector<std::string> segments;
    str::splitStringDelim(checkpointIdStr, &segments, '/');
    uassert(75800,
            fmt::format(
                "checkpointId unexpected segment count: {}, {}", segments.size(), checkpointIdStr),
            segments.size() == 4);
    uassert(75801,
            fmt::format("checkpointId unexpected prefix: {}", segments[0]),
            segments[0] == kCheckpoint);
    uassert(75802,
            fmt::format("unexpected tenantId: {}", segments[1]),
            segments[1] == _context->tenantId);
    uassert(75803,
            fmt::format("unexpected streamProcessorId: {}", segments[2]),
            segments[2] == _context->streamProcessorId);
    return CheckpointId{std::stoll(segments[3])};
}

boost::optional<CheckpointInfo> MongoDBCheckpointStorage::doReadCheckpointInfo(
    CheckpointId checkpointId) {
    auto findDoc = make_document(
        kvp(std::string{kId}, getFullCheckpointId(_checkpointDocIdPrefix, checkpointId)));
    mongocxx::options::find checkpointFindOpts;
    try {
        auto result = _collection->find_one(findDoc.view(), checkpointFindOpts);
        if (!result) {
            return boost::none;
        }
        return CheckpointInfo::parseOwned(_parserContext, fromBsoncxxDocument(std::move(*result)));
    } catch (const mongocxx::exception& e) {
        LOGV2_ERROR(
            8112606, "Failure while reading from checkpoint storage.", "exception"_attr = e.what());
        uasserted(8112607, "Failure while reading from checkpoint storage.");
    }
}

}  // namespace streams
