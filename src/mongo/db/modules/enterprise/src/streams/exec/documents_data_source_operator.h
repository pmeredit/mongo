/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/query/datetime/date_time_support.h"
#include "mongo/platform/random.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/generated_data_source_operator.h"
#include "streams/exec/message.h"
#include "streams/exec/source_operator.h"

namespace streams {

/**
 * This $source allows customers to try out the query syntax
 * without having an Atlas DB or Kafka topic to connect to.
 * It generates a predefined list of documents.
 */
class DocumentsDataSourceOperator : public GeneratedDataSourceOperator {
public:
    struct Options : public SourceOperator::Options {
        Options(SourceOperator::Options baseOptions)
            : SourceOperator::Options(std::move(baseOptions)) {}

        Options() = default;

        // The predefined list of documents to send.
        std::vector<mongo::Document> documents;
        // The number of documents to send in each call to doRunOnce().
        int docsPerRun{kDataMsgMaxDocSize};
    };

    DocumentsDataSourceOperator(Context* context, Options options)
        : GeneratedDataSourceOperator(context, /* numOutputs */ 1), _options(std::move(options)) {}

private:
    std::string doGetName() const override {
        return "DocumentsDataSourceOperator";
    }

    const SourceOperator::Options& getOptions() const override {
        return _options;
    }

    std::vector<StreamMsgUnion> getMessages(mongo::WithLock) override;

    const Options _options;
    // The index of the next document to generate.
    size_t _documentIdx{0};
};

}  // namespace streams
