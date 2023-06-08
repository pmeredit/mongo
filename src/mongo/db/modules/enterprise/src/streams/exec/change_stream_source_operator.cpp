/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/change_stream_source_operator.h"

#include <bsoncxx/json.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <mongocxx/pipeline.hpp>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "streams/exec/context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/document_timestamp_extractor.h"
#include "streams/exec/mongocxx_utils.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/util.h"

using namespace mongo;

namespace streams {

ChangeStreamSourceOperator::ChangeStreamSourceOperator(Options options)
    : SourceOperator(/*numInputs*/ 0, /*numOutputs*/ 1), _options(std::move(options)) {
    invariant(_options.svcCtx);
    _instance = getMongocxxInstance(_options.svcCtx);
    _uri = std::make_unique<mongocxx::uri>(_options.uri);
    _client = std::make_unique<mongocxx::client>(*_uri);

    // TODO SERVER-77563: This should account for tenantId.
    const auto& nss = _options.nss;
    auto db = nss.db();
    tassert(7596201, "Expected a non-empty database name", !db.empty());
    _database = std::make_unique<mongocxx::database>(_client->database(db.toString()));
    if (auto coll = nss.coll(); !coll.empty()) {
        _collection =
            std::make_unique<mongocxx::collection>(_database->collection(coll.toString()));
    }
}

void ChangeStreamSourceOperator::doStart() {
    // Establish our change stream cursor.
    if (_collection) {
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _collection->watch(mongocxx::pipeline(), _options.changeStreamOptions));
    } else {
        tassert(7596202,
                "Change stream $source is only supported against a collection or a database",
                _database);
        _changeStreamCursor = std::make_unique<mongocxx::change_stream>(
            _database->watch(mongocxx::pipeline(), _options.changeStreamOptions));
    }
    _it = mongocxx::change_stream::iterator();
}

void ChangeStreamSourceOperator::doStop() {
    // Destroy our iterator and close our cursor.
    _it = mongocxx::change_stream::iterator();
    _changeStreamCursor = nullptr;
}

int32_t ChangeStreamSourceOperator::doRunOnce() {
    // TODO SERVER-77657: Handle invalidate events.
    invariant(_changeStreamCursor);
    if (_it == mongocxx::change_stream::iterator()) {
        _it = _changeStreamCursor->begin();
    }

    StreamDataMsg dataMsg;
    int64_t totalNumInputBytes = 0;

    // If our cursor is exhausted, break from the loop and wait until the next call to
    // 'doRunOnce' to try reading from '_changeStreamCursor' again.
    while (_it != _changeStreamCursor->end() && dataMsg.docs.size() < _options.maxNumDocsToReturn) {
        // TODO SERVER-77563: This conversion from bsoncxx to BSONObj can be improved.
        mongo::BSONObj changeEventObj(mongo::fromjson(bsoncxx::to_json(*_it)));

        // Advance our cursor before processing the current document.
        ++_it;
        int64_t inputBytes = changeEventObj.objsize();

        if (auto streamDoc = processChangeEvent(std::move(changeEventObj)); streamDoc) {
            dataMsg.docs.push_back(std::move(*streamDoc));
            totalNumInputBytes += inputBytes;
        }
    }

    // If we've hit the end of our cursor, set our iterator to the default iterator so that we can
    // reset it on the next call to doRunOnce.
    if (_it == _changeStreamCursor->end()) {
        _it = mongocxx::change_stream::iterator();
    }

    incOperatorStats(OperatorStats{.numInputDocs = int64_t(dataMsg.docs.size()),
                                   .numInputBytes = totalNumInputBytes});

    boost::optional<StreamControlMsg> newControlMsg = boost::none;
    if (_options.watermarkGenerator) {
        newControlMsg = StreamControlMsg{_options.watermarkGenerator->getWatermarkMsg()};
        if (*newControlMsg == _lastControlMsg) {
            newControlMsg = boost::none;
        } else {
            _lastControlMsg = *newControlMsg;
        }
    }

    int32_t docsSent = dataMsg.docs.size();
    sendDataMsg(0, std::move(dataMsg), std::move(newControlMsg));
    return docsSent;
}


// Obtain the 'ts' field from either:
// - 'timeField' if the user specified
// - The 'wallTime' field (if we're reading from a change stream against a cluster whose
// version is GTE 6.0).
// - The 'clusterTime' (if we're reading from a change stream against a cluster whose
// version is LT 6.0).
//
// Then, does additional work to generate a watermark and verify that the ts output field doesn't
// already exist. Returns boost::none if a timestamp could not be obtained.
boost::optional<mongo::Date_t> ChangeStreamSourceOperator::getTimestamp(
    const mongo::BSONObj& changeEventObj) {
    mongo::Date_t ts;
    Document tempChangeEventDoc(changeEventObj);
    if (_options.timestampExtractor) {
        try {
            ts = _options.timestampExtractor->extractTimestamp(tempChangeEventDoc);
        } catch (const DBException& e) {
            _options.context->dlq->addMessage(
                toDeadLetterQueueMsg(std::move(tempChangeEventDoc), e.toString()));
            return boost::none;
        }
    } else if (auto wallTime = tempChangeEventDoc[DocumentSourceChangeStream::kWallTimeField];
               !wallTime.missing()) {
        if (wallTime.getType() != BSONType::Date) {
            _options.context->dlq->addMessage(
                toDeadLetterQueueMsg(std::move(tempChangeEventDoc),
                                     std::string{"Change event's wall time was not a date"}));
            return boost::none;
        } else {
            ts = wallTime.getDate();
        }
    } else {
        auto clusterTime = tempChangeEventDoc[DocumentSourceChangeStream::kClusterTimeField];
        if (clusterTime.missing()) {
            _options.context->dlq->addMessage(
                toDeadLetterQueueMsg(std::move(tempChangeEventDoc),
                                     std::string{"Change event did not have clusterTime"}));
            return boost::none;
        } else if (clusterTime.getType() != BSONType::bsonTimestamp) {
            _options.context->dlq->addMessage(toDeadLetterQueueMsg(
                std::move(tempChangeEventDoc),
                std::string{"Change event's clusterTime was not a timestamp."}));
            return boost::none;
        } else {
            ts = Date_t::fromMillisSinceEpoch(clusterTime.getTimestamp().asInt64());
        }
    }

    if (_options.watermarkGenerator) {
        if (_options.watermarkGenerator->isLate(ts.toMillisSinceEpoch())) {
            _options.context->dlq->addMessage(toDeadLetterQueueMsg(
                std::move(tempChangeEventDoc), std::string{"Input document arrived late."}));
            return boost::none;
        }
        _options.watermarkGenerator->onEvent(ts.toMillisSinceEpoch());
    }

    // Verify that 'changeStreamDoc' doesn't already have a value for 'timestampOutputFieldName'.
    // TODO SERVER-77563: Consider rewriting this to overwrite an existing _ts field.
    const auto& tsOutField = _options.timestampOutputFieldName;
    if (auto tsOutFieldValue = tempChangeEventDoc[tsOutField]; !tsOutFieldValue.missing()) {
        _options.context->dlq->addMessage(toDeadLetterQueueMsg(
            std::move(tempChangeEventDoc),
            std::string{"Error: timestamp output field " + tsOutField + " already exists"}));
        return boost::none;
    }

    return ts;
}

boost::optional<StreamDocument> ChangeStreamSourceOperator::processChangeEvent(
    mongo::BSONObj changeStreamObj) {
    auto maybeTimestamp = getTimestamp(changeStreamObj);
    if (!maybeTimestamp) {
        return boost::none;
    }

    Date_t ts = *maybeTimestamp;

    BSONObjBuilder objBuilder(std::move(changeStreamObj));

    // Append 'ts' to 'objBuilder'.
    objBuilder.appendDate(_options.timestampOutputFieldName, ts);

    StreamDocument streamDoc(Document(objBuilder.obj()));

    // TODO SERVER-77563: Expand data added to 'streamMeta' once this is clarified in the design.
    streamDoc.streamMeta.setSourceType(StreamMetaSourceTypeEnum::Atlas);
    streamDoc.streamMeta.setTimestamp(ts);

    streamDoc.minProcessingTimeMs = curTimeMillis64();
    streamDoc.minEventTimestampMs = ts.toMillisSinceEpoch();
    streamDoc.maxEventTimestampMs = ts.toMillisSinceEpoch();
    return streamDoc;
}
}  // namespace streams
