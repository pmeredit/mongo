#pragma once

#include <boost/optional.hpp>
#include <memory>
#include <string>
#include <vector>

#include "mongo/db/operation_context.h"
#include "mongo/db/pipeline/expression_context.h"
#include "streams/exec/dead_letter_queue.h"
#include "streams/exec/output_sampler.h"

namespace streams {

// Encapsulates the top-level state of a stream processor.
struct Context {
    std::string streamName;
    std::string clientName;
    mongo::ServiceContext::UniqueClient client;
    mongo::ServiceContext::UniqueOperationContext opCtx;
    boost::intrusive_ptr<mongo::ExpressionContext> expCtx;
    std::unique_ptr<DeadLetterQueue> dlq;
    bool isEphemeral{false};
};

}  // namespace streams
