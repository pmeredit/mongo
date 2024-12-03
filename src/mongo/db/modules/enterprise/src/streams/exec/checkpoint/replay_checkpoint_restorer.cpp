/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include <boost/none.hpp>

#include "streams/exec/checkpoint/replay_checkpoint_restorer.h"
#include "streams/exec/message.h"

using namespace mongo;

namespace streams {

boost::optional<Document> ReplayCheckpointRestorer::getNextRecord(OperatorId opId) {
    // The replay checkpoint only contains the $source state.
    if (opId == 0 /* $source */) {
        return Document(std::move(_replaySourceState.getOwned()));
    }
    return boost::none;
}

}  // namespace streams
