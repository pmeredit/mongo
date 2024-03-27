#pragma once

#include <mongocxx/exception/exception.hpp>
#include <string>
#include <tuple>
#include <vector>

#include "mongo/db/pipeline/document_source.h"
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
 * The operator for $merge.
 */
// TODO: DocumentSourceMerge does internal buffering. We may want to explicitly flush in some cases
// or have a timeout on this buffering.
class MergeOperator : public QueuedSinkOperator {
public:
    struct Options {
        // DocumentSource stage that this Operator wraps.
        mongo::DocumentSourceMerge* documentSource;
        mongo::NameExpression db;
        mongo::NameExpression coll;
        boost::optional<std::set<mongo::FieldPath>> onFieldPaths;
        boost::intrusive_ptr<mongo::ExpressionContext> mergeExpCtx;
    };

    MergeOperator(Context* context, Options options);

    mongo::DocumentSourceMerge* documentSource() {
        return _options.documentSource;
    }

protected:
    std::string doGetName() const override {
        return "MergeOperator";
    }

    OperatorStats processDataMsg(StreamDataMsg dataMsg) override;

    void validateConnection() override;

private:
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

    Options _options;
    mongo::MergeProcessor* _processor{nullptr};

    // Set only if the $merge has a literal target.
    boost::optional<std::set<mongo::FieldPath>> _literalMergeOnFieldPaths;
};

}  // namespace streams
