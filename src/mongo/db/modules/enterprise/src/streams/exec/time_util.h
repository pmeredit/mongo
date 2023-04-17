#pragma once

#include "streams/exec/window_stage_gen.h"

namespace streams {

int64_t toMillis(mongo::StreamTimeUnitEnum unit, int count);

}  // namespace streams
