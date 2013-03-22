/*
 * Copyright (C) 2012 10gen, Inc.  All Rights Reserved.
 */

#include <cerrno>
#include <dlfcn.h>
#include <gcrypt.h>

#include "mongo/base/init.h"
#include "mongo/base/status.h"

namespace mongo {
namespace {

    // Declaration of functions and structures for gcrypt pthread support.  From gcrypt.h.
    GCRY_THREAD_OPTION_PTHREAD_IMPL;

    /**
     * Initializes the gcrypt library, if and only if mongo is linked against it dynamically.
     * This may happen if libgsasl is built to use cryptographic functions from libgcrypt.
     */
    MONGO_INITIALIZER_GENERAL(
        GcryptLibrary,
        MONGO_NO_PREREQUISITES,
        ("SaslAuthenticationLibrary", "SaslClientContext"))(InitializerContext*) {

        typedef gcry_error_t (*gcry_control_fn)(enum gcry_ctl_cmds, ...);
        typedef const char* (*gcry_check_version_fn)(const char*);

        // Mask the function declarations from gcrypt.h.  We must dynamically load these symbols
        // with dlsym, to avoid introducing a link-time dependency on libgcrypt.  We assume that if
        // they are not available dynamically, then the libgsasl library does not depend on them.
        // This assumption would be violated if mongo servers were statically linked against
        // libgcrypt.
        gcry_check_version_fn gcry_check_version = NULL;
        gcry_control_fn gcry_control = NULL;

        dlerror();
        gcry_check_version = reinterpret_cast<gcry_check_version_fn>(
                dlsym(RTLD_DEFAULT, "gcry_check_version"));
        const char* dlError = dlerror();
        if (dlError) {
            // No gcry_check_version symbol found, means we're not linked against gcrypt, so we're
            // done!
            return Status::OK();
        }
        if (gcry_check_version == NULL) {
            return Status(ErrorCodes::UnknownError,
                          "gcrypt library contains a bad implementation of gcry_check_version");
        }

        gcry_control = reinterpret_cast<gcry_control_fn>(dlsym(RTLD_DEFAULT, "gcry_control"));
        dlError = dlerror();
        if (dlError || gcry_control == NULL) {
            return Status(ErrorCodes::UnknownError,
                          "gcrypt library contains no useful implementation of gcry_control");
        }

        // We must be the first initializers of the gcrypt library.
        if (gcry_control(GCRYCTL_INITIALIZATION_FINISHED_P)) {
            return Status(ErrorCodes::AlreadyInitialized, "gcrypt library already initialized.");
        }

        // Registers that this application is multithreaded with pthreads.
        if (GPG_ERR_NO_ERROR != gcry_control(GCRYCTL_SET_THREAD_CBS, &gcry_threads_pthread)) {
            return Status(ErrorCodes::UnknownError,
                          "Could not set gcrypt thread callbacks.");
        }

        // Disabling secmem precludes FIPS support in the gcrypt library.  However, support for
        // secmem (secure memory) raises design questions, since it may require that mongo
        // servers be started as root or with elevated privileges.
        if (GPG_ERR_NO_ERROR != gcry_control(GCRYCTL_DISABLE_SECMEM)) {
            return Status(ErrorCodes::UnknownError,
                          "Could not disable gcrypt secure memory support.");
        }

        // Mongo doesn't use gcrypt directly, and does not care about the version.  The gsasl
        // library, which may use it, is responsible for checking the version.  However, we must
        // call gcry_check_version, in order to perform some library initialization before
        gcry_check_version(NULL);

        if (GPG_ERR_NO_ERROR != gcry_control(GCRYCTL_INITIALIZATION_FINISHED)) {
            return Status(ErrorCodes::UnknownError,
                          "Could not fully initialize gcrypt library.");
        }

        return Status::OK();
    }

}  // namespace
}  // namespace mongo
