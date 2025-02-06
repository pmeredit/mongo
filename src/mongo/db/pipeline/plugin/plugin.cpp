#include "mongo/db/pipeline/plugin/plugin.h"

extern "C" {

extern void initialize_rust_plugins(mongodb_plugin_portal* plugin_portal);

// Invoked when a plugin is loaded to allow the plugin to register services.
//
// A function with this signature would be called in each plugin shared object loaded.
// TODO: extend this to allow passing arguments during plugin initialization. This could be used
// to enable features or specify remote backends.
void mongodb_initialize_plugin(mongodb_plugin_portal* plugin_portal) {
    initialize_rust_plugins(plugin_portal);
}

}  // extern "C"