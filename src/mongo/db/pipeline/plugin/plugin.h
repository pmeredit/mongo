#pragma once

#include "mongo/db/pipeline/plugin/api.h"

extern "C" {

// Invoked when a plugin is loaded to allow the plugin to register services.
//
// A function with this signature would be called in each plugin shared object loaded.
// TODO: extend this to allow passing arguments during plugin initialization. This could be used
// to enable features or specify remote backends.
void mongodb_initialize_plugin(mongodb_plugin_portal* plugin_portal);

}  // extern "C"