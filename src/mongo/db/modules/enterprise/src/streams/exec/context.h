#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/expression_context.h"
#include "streams/exec/output_sampler.h"

namespace streams {

// Encapsulates metadata for an OutputSampler.
struct OutputSamplerInfo {
    int64_t cursorId{0};
    boost::intrusive_ptr<OutputSampler> outputSampler;
};

// Encapsulates the top-level state of a stream processor.
struct Context {
    std::string streamName;
    std::string clientName;
    mongo::ServiceContext::UniqueClient client;
    mongo::ServiceContext::UniqueOperationContext opCtx;
    boost::intrusive_ptr<mongo::ExpressionContext> expCtx;
    // The list of active OutputSamplers created for the ongoing sample() requests.
    std::vector<OutputSamplerInfo> outputSamplers;
    // Last cursor id used for a sample request.
    int64_t lastCursorId{0};
};

}  // namespace streams
