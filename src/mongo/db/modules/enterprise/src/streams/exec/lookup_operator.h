#pragma once

#include "mongo/db/namespace_string.h"
#include "mongo/util/chunked_memory_aggregator.h"
#include "streams/exec/message.h"
#include "streams/exec/mongodb_process_interface.h"
#include "streams/exec/operator.h"

namespace mongo {
class DocumentSourceLookUp;
}  // namespace mongo

namespace streams {

struct Context;

/**
 * The operator for $lookup.
 */
class LookUpOperator : public Operator {
public:
    struct Options {
        // DocumentSourceLookUp stage that this Operator wraps. This object is not used for actual
        // document processing but is only used for accessing its member fields.
        mongo::DocumentSourceLookUp* documentSource;
        // MongoDBProcessInterface is not thread-safe and we should use a separate instance for each
        // thread. The main thread's MongoDBProcessInterface may be used for $merge's consumerLoop
        // and so we should use separate instances for $lookup's from collection.
        std::shared_ptr<MongoDBProcessInterface> foreignMongoDBClient;
        mongo::NamespaceString foreignNs;
    };

    LookUpOperator(Context* context, Options options);

    mongo::DocumentSourceLookUp* documentSource() {
        return _options.documentSource;
    }

protected:
    std::string doGetName() const override {
        return "LookUpOperator";
    }
    void doOnDataMsg(int32_t inputIdx,
                     StreamDataMsg dataMsg,
                     boost::optional<StreamControlMsg> controlMsg) override;
    void doOnControlMsg(int32_t inputIdx, StreamControlMsg controlMsg) override;

private:
    using PipelinePtr = std::unique_ptr<mongo::Pipeline, mongo::PipelineDeleter>;

    // Creates a pipeline to fetch matching documents from the foreign collection for the given doc.
    // If a mongocxx exception is encountered, it returns boost::none and adds the current document
    // to the dead letter queue.
    PipelinePtr buildPipeline(const StreamDocument& streamDoc);

    // Returns all the documents from the given pipeline. If a mongocxx exception is encountered, it
    // returns boost::none and adds the current document to the dead letter queue.
    boost::optional<std::vector<mongo::Value>> getAllDocsFromPipeline(
        const StreamDocument& streamDoc, PipelinePtr pipelin);

    // Returns the next document from '_pipeline'. Caller should ensure that the '_pipeline' has not
    // reached the end yet. If a mongocxx exception is encountered, it returns boost::none and adds
    // the current document to the dead letter queue.
    boost::optional<mongo::Value> getNextDocFromPipeline(const StreamDocument& streamDoc);

    // Joins the given Document and Value to produce a joined doc.
    mongo::Document produceJoinedDoc(mongo::Document inputDoc, mongo::Value asFieldValue);

    Options _options;
    mongo::FieldPath _localField;
    mongo::FieldPath _foreignField;
    mongo::FieldPath _asField;
    // Whether this operator should also unwind the 'as' field in the joined doc.
    bool _shouldUnwind{false};
    // When _shouldUnwind is true, this tracks the value of includeArrayIndex from the $unwind
    // stage.
    boost::optional<mongo::FieldPath> _unwindIndexPath;
    // When _shouldUnwind is true, this tracks the value of preserveNullAndEmptyArrays from the
    // $unwind stage.
    bool _unwindPreservesNullAndEmptyArrays{false};
    // Additional filter extracted from the following $match stage, if any, for the foreign
    // collection. Note that this is only initialized when '_shouldUnwind' is true.
    mongo::BSONObj _additionalFilter;
    // When _shouldUnwind is true, this tracks index of the unwound array element.
    int32_t _unwindCurIndex{0};
    // The pipeline for the last join operation. Note that these are only initialized when
    // '_shouldUnwind' is true.
    PipelinePtr _pipeline;
    mongo::MemoryUsageHandle _memoryUsageHandle;

    // The ExpressionContext that is used when performing aggregation pipelines against the remote
    // db.
    boost::intrusive_ptr<mongo::ExpressionContext> _fromExpCtx;
};

}  // namespace streams
