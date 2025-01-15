/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/none.hpp>
#include <boost/optional/optional.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/exec/document_value/document.h"
#include "mongo/db/exec/document_value/value.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/pipeline.h"
#include "mongo/db/pipeline/stage_constraints.h"
#include "mongo/db/pipeline/variables.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/mongodb_process_interface.h"

namespace streams {
using mongo::BSONElement;
using mongo::BSONObj;
using mongo::DocumentSource;
using mongo::OperationContext;
using mongo::Pipeline;
using mongo::SerializationOptions;
using mongo::SpecificStats;
using mongo::StageConstraints;
using mongo::StringData;
using mongo::operator""_sd;
using mongo::Value;
using mongo::Variables;

/**
 * Executes the given pipeline on the remote MongoDB server and returns the results.
 */
class DocumentSourceRemoteDbCursor : public DocumentSource {
public:
    static constexpr size_t kDefaultBatchSize = 1000;

    static constexpr StringData kStageName = "$_remoteDbCursor"_sd;

    static boost::intrusive_ptr<DocumentSourceRemoteDbCursor> create(
        MongoDBProcessInterface* procItf, const Pipeline* pipeline);

    const char* getSourceName() const override;

    static const Id& id;

    Id getId() const override {
        return id;
    }

    Value serialize(const SerializationOptions& opts = SerializationOptions{}) const final {
        // This is an internal-only stage and so this should never be called.
        MONGO_UNREACHABLE;
    }

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kFirst,
                                     HostTypeRequirement::kAnyShard,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kNotAllowed,
                                     TransactionRequirement::kNotAllowed,
                                     LookupRequirement::kNotAllowed,
                                     UnionRequirement::kNotAllowed);
        constraints.requiresInputDocSource = false;
        return constraints;
    }

    boost::optional<DistributedPlanLogic> distributedPlanLogic() final {
        return boost::none;
    }

    // No-op for lock-yielding since this does not involved any underlying collection.
    void detachFromOperationContext() final {}

    // No-op for lock-yielding since this does not involved any underlying collection.
    void reattachToOperationContext(OperationContext* opCtx) final {}

    const SpecificStats* getSpecificStats() const final {
        return nullptr;
    }

    BSONObj serializeToBSONForDebug() const final {
        return BSON(kStageName << "{}");
    }

    void addVariableRefs(std::set<Variables::Id>* refs) const final {
        // This is an internal-only stage and so this should never be called.
        MONGO_UNREACHABLE;
    }

protected:
    DocumentSourceRemoteDbCursor(MongoDBProcessInterface* procItf, const Pipeline* pipeline);

    GetNextResult doGetNext() final;

    ~DocumentSourceRemoteDbCursor() override;

    void doDispose() final;

private:
    MongoDBProcessInterface* _procItf;
    // Stores reply from the last command so that '_batch' can point to the internal buffer of
    // '_reply'.
    BSONObj _reply;
    int64_t _cursorId{0};
    std::vector<BSONElement> _batch;
    std::vector<BSONElement>::const_iterator _batchIter;
};
}  // namespace streams
