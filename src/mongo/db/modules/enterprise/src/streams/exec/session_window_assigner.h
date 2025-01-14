/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/pipeline/expression.h"
#include "streams/exec/stages_gen.h"
#include "streams/exec/util.h"
#include "streams/exec/window_assigner.h"

namespace streams {

/**
 * SessionWindowAssigner is a stateless class used by WindowAwareOperator to assign timestamps to
 * define the bounds for session windows.
 */
class SessionWindowAssigner : public WindowAssigner {
public:
    struct Options : public WindowAssigner::Options {
        Options(WindowAssigner::Options baseOptions)
            : WindowAssigner::Options(std::move(baseOptions)) {}

        int64_t gapSize{0};
        mongo::StreamTimeUnitEnum gapUnit;
        boost::intrusive_ptr<mongo::Expression> partitionBy;
    };

    SessionWindowAssigner(Options options)
        : WindowAssigner(options),
          _partitionBy(options.partitionBy),
          _windowGapSizeMs(toMillis(options.gapUnit, options.gapSize)) {}

    bool shouldCloseWindow(int64_t windowEndTime,
                           int64_t inputWatermarkMinusLateness) const override;

    // Returns true if an interval overlaps with another interval.
    bool shouldMergeSessionWindows(int64_t start1,
                                   int64_t end1,
                                   int64_t start2,
                                   int64_t end2) const;

    const boost::intrusive_ptr<mongo::Expression>& getPartitionBy() const {
        return _partitionBy;
    }

    int64_t getGap() const {
        return _windowGapSizeMs;
    }

private:
    const boost::intrusive_ptr<mongo::Expression> _partitionBy;
    const int64_t _windowGapSizeMs;
};

}  // namespace streams
