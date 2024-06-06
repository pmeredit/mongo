/*
 * Copyright (C) 2013-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo_gssapi.h"

// `time.h` needs to be included first on some systems, to provide `time_t` for gssapi headers.
#include <ctime>

#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"

#include "util/gssapi_helpers.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {
namespace gssapi {

namespace {

/**
 * Convert the name described the "name" into the canonical for for the given "nameType".
 *
 * For example, if the name type is GSS_C_NT_USER_NAME, the name supplied is "andy", and this
 * process is running in the "10GEN.ME" realm, the canonical name returned will be
 * "andy@10GEN.ME".  However, if "andy@EXAMPLE.COM" is supplied, the canonical name will be
 * "andy@EXAMPLE.COM".
 *
 * Returns a StatusWith<std::string> containing the canonical name on success, or an error.
 */
StatusWith<std::string> canonicalizeName(gss_OID nameType, StringData name) {
    OM_uint32 majorStatus = 0;
    OM_uint32 minorStatus = 0;
    gss_buffer_desc nameBuffer;
    nameBuffer.value = const_cast<char*>(name.rawData());
    nameBuffer.length = name.size();
    gss_name_t gssNameInput = GSS_C_NO_NAME;
    gss_name_t gssNameCanonical = GSS_C_NO_NAME;
    gss_buffer_desc bufFullName = {0};
    StatusWith<std::string> status(ErrorCodes::InternalError, "SHOULD NEVER BE RETURNED");

    majorStatus = gss_import_name(&minorStatus, &nameBuffer, nameType, &gssNameInput);
    if (GSS_ERROR(majorStatus))
        goto error;

    majorStatus = gss_canonicalize_name(
        &minorStatus, gssNameInput, const_cast<gss_OID>(gss_mech_krb5), &gssNameCanonical);
    if (GSS_ERROR(majorStatus))
        goto error;

    majorStatus = gss_display_name(&minorStatus, gssNameCanonical, &bufFullName, nullptr);
    if (GSS_ERROR(majorStatus)) {
        goto error;
    } else {
        auto s = std::string(static_cast<const char*>(bufFullName.value), bufFullName.length);
        status = StatusWith<std::string>(s);
        goto done;
    }

error:
    status =
        StatusWith<std::string>(ErrorCodes::UnknownError,
                                str::stream() << "Could not canonicalize \"" << name << "\"; "
                                              << getGssapiErrorString(majorStatus, minorStatus));

done:
    if (gssNameInput != GSS_C_NO_NAME)
        fassert(4005, !GSS_ERROR(gss_release_name(&minorStatus, &gssNameInput)));
    if (gssNameCanonical != GSS_C_NO_NAME)
        fassert(4006, !GSS_ERROR(gss_release_name(&minorStatus, &gssNameCanonical)));
    if (bufFullName.value)
        fassert(4007, !GSS_ERROR(gss_release_buffer(&minorStatus, &bufFullName)));

    return status;
}
}  // namespace

StatusWith<std::string> canonicalizeUserName(StringData name) {
    return canonicalizeName(GSS_C_NT_USER_NAME, name);
}

StatusWith<std::string> canonicalizeServerName(StringData name) {
    return canonicalizeName(GSS_C_NT_HOSTBASED_SERVICE, name);
}

Status tryAcquireServerCredential(const std::string& principalName) {
    OM_uint32 majorStatus;
    OM_uint32 minorStatus;
    gss_buffer_desc nameBuffer;
    gss_name_t gssPrincipalName = GSS_C_NO_NAME;
    gss_cred_id_t credential = GSS_C_NO_CREDENTIAL;

    auto statusOrValue = canonicalizeServerName(principalName);
    Status status = statusOrValue.getStatus();

    if (status.isOK()) {
        auto& canonicalPrincipalName = statusOrValue.getValue();
        nameBuffer.value = const_cast<char*>(canonicalPrincipalName.c_str());
        nameBuffer.length = canonicalPrincipalName.size();

        minorStatus = 0;
        majorStatus =
            gss_import_name(&minorStatus, &nameBuffer, GSS_C_NT_USER_NAME, &gssPrincipalName);
        if (GSS_ERROR(majorStatus)) {
            status =
                Status(ErrorCodes::UnknownError,
                       str::stream() << "gssapi could not import name " << principalName << "; "
                                     << getGssapiErrorString(majorStatus, minorStatus));
            goto done;
        }

        minorStatus = 0;
        majorStatus = gss_acquire_cred(&minorStatus,        // [out] minor_status
                                       gssPrincipalName,    // desired_name
                                       0,                   // time_req
                                       GSS_C_NULL_OID_SET,  // desired_mechs
                                       GSS_C_ACCEPT,        // cred_usage
                                       &credential,         // output_cred_handle
                                       nullptr,             // actual_mechs
                                       nullptr);            // time_rec
        if (GSS_ERROR(majorStatus)) {
            status = Status(ErrorCodes::UnknownError,
                            str::stream() << "gssapi could not acquire server credential for "
                                          << canonicalPrincipalName << "; "
                                          << getGssapiErrorString(majorStatus, minorStatus));
        }
    }

done:
    if (gssPrincipalName != GSS_C_NO_NAME)
        fassert(4008, !GSS_ERROR(gss_release_name(&minorStatus, &gssPrincipalName)));
    if (credential != GSS_C_NO_CREDENTIAL)
        fassert(4009, !GSS_ERROR(gss_release_cred(&minorStatus, &credential)));

    return status;
}

}  // namespace gssapi
}  // namespace mongo
