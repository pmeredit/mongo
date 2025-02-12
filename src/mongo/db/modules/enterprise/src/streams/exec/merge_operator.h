/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <mongocxx/exception/exception.hpp>
#include <string>
#include <tuple>
#include <vector>

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_merge.h"
#include "mongo/db/pipeline/name_expression.h"
#include "mongo/stdx/unordered_map.h"
#include "streams/exec/queued_sink_operator.h"

namespace mongo {
class DocumentSourceMerge;
class MergeProcessor;
}  // namespace mongo

namespace streams {

struct Context;
class MetricManager;

/**
 * MergeOperator is the operator for the $merge stage. MergeOperator
 * is a QueuedSinkOperator, which manages one or more WriterThread.
 */
class MergeOperator : public QueuedSinkOperator {
public:
    struct Options {
        // Stage definition of the MergeOperator.
        mongo::MergeOperatorSpec spec;
        // Target Atlas options for the MergeOperator.
        mongo::AtlasConnectionOptions atlasOptions;
        // Set to true in some unit tests.
        bool isTest{false};
        // Set to false in some unit tests.
        bool allowMergeOnNullishValues{true};
    };

    MergeOperator(Context* context, Options options);

    // Make a SinkWriter instance.
    std::unique_ptr<SinkWriter> makeWriter() override;

    std::string doGetName() const override {
        return "MergeOperator";
    }

    mongo::ConnectionTypeEnum getConnectionType() const override {
        return mongo::ConnectionTypeEnum::Atlas;
    }

private:
    friend class MergeOperatorTest;

    boost::intrusive_ptr<mongo::DocumentSource> makeDocumentSourceMerge();
    boost::intrusive_ptr<mongo::ExpressionContext> makeExpressionContext();
    mongo::AtlasCollection parseAtlasConnection();

    // Options for this operator.
    Options _options;

    // Parse $merge.on field paths.
    boost::optional<std::set<mongo::FieldPath>> _onFieldPaths;
};

/**
 * MergeWriter implements the logic for $merge into a target collection.
 * One MergeOperator (QueuedSinkOperator) might manages multiple MergeWriter instances
 * if $merge.parallelism > 1.
 */
class MergeWriter : public SinkWriter {
public:
    struct Options {
        // DocumentSource stage that this Writer wraps.
        boost::intrusive_ptr<mongo::DocumentSourceMerge> documentSource;
        // Expression context.
        boost::intrusive_ptr<mongo::ExpressionContext> mergeExpCtx;
        // Database to write to.
        mongo::NameExpression db;
        // Collection to write to.
        mongo::NameExpression coll;
        // $merge.on fields to merge based on.
        boost::optional<std::set<mongo::FieldPath>> onFieldPaths;
    };

    MergeWriter(Context* context, SinkOperator* sinkOperator, Options options);

    mongo::DocumentSourceMerge* documentSource() {
        return _options.documentSource.get();
    }

protected:
    OperatorStats processDataMsg(StreamDataMsg dataMsg) override;

    void validateConnection() override;

    size_t partition(StreamDocument& doc) override;

private:
    friend class MergeOperatorTest;

    using NsKey = std::pair<std::string, std::string>;
    using DocIndices = std::vector<size_t>;
    using DocPartitions = mongo::stdx::unordered_map<NsKey, DocIndices>;

    // End the operator in error.
    void errorOut(const mongocxx::exception& e, const mongo::NamespaceString& outputNs);

    std::tuple<DocPartitions, OperatorStats> partitionDocsByTargets(const StreamDataMsg& dataMsg);

    // Processes the docs at the indexes in 'docIndices' each of which points to a doc in 'dataMsg'.
    // This returns the stats for the batch of messages passed in.
    OperatorStats processStreamDocs(const StreamDataMsg& dataMsg,
                                    const mongo::NamespaceString& outputNs,
                                    const std::vector<size_t>& docIndices,
                                    size_t maxBatchDocSize);

    // Returns an error message prefix for the output namespace.
    std::string getErrorPrefix(const mongo::NamespaceString& outputNs);

    Options _options;
    mongo::MergeProcessor* _processor{nullptr};

    // Set only if the $merge has a literal target.
    boost::optional<std::set<mongo::FieldPath>> _literalMergeOnFieldPaths;

    // Used to hash docs on their $merge.on fields, to route between threads.
    mongo::ValueComparator::Hasher _hash;
};

}  // namespace streams
