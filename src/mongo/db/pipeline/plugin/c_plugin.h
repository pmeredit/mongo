#pragma once

#include "mongo/db/pipeline/plugin/api.h"

namespace mongo {

// Register all plugins defined in C/C++.
void initialize_c_plugins(mongodb_plugin_portal* plugin_portal);

}  // namespace mongo