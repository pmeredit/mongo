/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

#include "mongo_gssapi.h"

#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
namespace gssapi {

namespace {

    /**
     * Write what the GSSAPI library considers to be the user-displayable description of "inStatus"
     * to "os".
     *
     * "statusType" indicates whether "inStatus" is a major status code (GSS_C_GSS_CODE) or minor
     * status code (GSS_C_MECH_CODE).  When it is a minor code, this function always assumes that
     * the mechanism is gss_mech_krb5.
     */
    void putGssapiStatusString(mongoutils::str::stream* os,
                               OM_uint32 inStatus,
                               int statusType) {
        OM_uint32 majorStatus;
        OM_uint32 minorStatus;
        OM_uint32 context = 0;
        gss_buffer_desc message = { 0 };

        if (statusType == GSS_C_GSS_CODE) {
            *os << "Major code " << inStatus << "; ";
        }
        else if (statusType == GSS_C_MECH_CODE) {
            *os << "Minor code " << inStatus << "; ";
        }
        do {
            majorStatus = gss_display_status(&minorStatus,
                                             inStatus,
                                             statusType,
                                             const_cast<gss_OID>(gss_mech_krb5),
                                             &context,
                                             &message);
            if (!GSS_ERROR(majorStatus)) {
                *os << (message.length > 0 ?
                        StringData(static_cast<const char*>(message.value), message.length) :
                        StringData("(no data)")) << "; ";
                fassert(4010, !GSS_ERROR(gss_release_buffer(&minorStatus, &message)));
            }
            else {
                *os << "gss_display_status() failed on context value " << context << "; ";
                context = 0;
            }
        } while (context != 0);
    }

    /**
     * Return a string describing the GSSAPI error described by "inMajorStatus" and "inMinorStatus."
     */
    std::string getGssapiErrorString(OM_uint32 inMajorStatus, OM_uint32 inMinorStatus) {
        mongoutils::str::stream os;
        putGssapiStatusString(&os, inMajorStatus, GSS_C_GSS_CODE);
        putGssapiStatusString(&os, inMinorStatus, GSS_C_MECH_CODE);
        return os;
    }

    /**
     * Convert the name described the "name" into the canonical for for the given "nameType".
     *
     * For example, if the name type is GSS_C_NT_USER_NAME, the name supplied is "andy", and this
     * process is running in the "10GEN.ME" realm, the canonicalName returned will be
     * "andy@10GEN.ME".  However, if "andy@EXAMPLE.COM" is supplied, the canonical name will be
     * "andy@EXAMPLE.COM".
     *
     * Result stored to "canonicalName" and returns Status::OK() on success.
     */
    Status canonicalizeName(gss_OID nameType, const StringData& name, std::string* canonicalName) {
        OM_uint32 majorStatus = 0;
        OM_uint32 minorStatus = 0;
        gss_buffer_desc nameBuffer;
        nameBuffer.value = const_cast<char*>(name.rawData());
        nameBuffer.length = name.size();
        gss_name_t gssNameInput = GSS_C_NO_NAME;
        gss_name_t gssNameCanonical = GSS_C_NO_NAME;
        gss_buffer_desc bufFullName = { 0 };
        Status status(ErrorCodes::InternalError, "SHOULD NEVER BE RETURNED");

        majorStatus = gss_import_name(&minorStatus,
                                      &nameBuffer,
                                      nameType,
                                      &gssNameInput);
        if (GSS_ERROR(majorStatus))
            goto error;

        majorStatus = gss_canonicalize_name(&minorStatus,
                                            gssNameInput,
                                            const_cast<gss_OID>(gss_mech_krb5),
                                            &gssNameCanonical);
        if (GSS_ERROR(majorStatus))
            goto error;

        majorStatus = gss_display_name(&minorStatus,
                                       gssNameCanonical,
                                       &bufFullName,
                                       NULL);
        if (GSS_ERROR(majorStatus))
            goto error;

        *canonicalName = std::string(static_cast<const char*>(bufFullName.value),
                                     bufFullName.length);
        status = Status::OK();
        goto done;

    error:
        status = Status(ErrorCodes::UnknownError,
                        mongoutils::str::stream() << "Could not canonicalize \"" << name <<
                        "\"; " << getGssapiErrorString(majorStatus, minorStatus));

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

    Status canonicalizeUserName(const StringData& name, std::string* canonicalName) {
        return canonicalizeName(GSS_C_NT_USER_NAME, name, canonicalName);
    }

    Status canonicalizeServerName(const StringData& name, std::string* canonicalName) {
        return canonicalizeName(GSS_C_NT_HOSTBASED_SERVICE, name, canonicalName);
    }

    Status tryAcquireServerCredential(const StringData& principalName) {
        OM_uint32 majorStatus;
        OM_uint32 minorStatus;
        gss_buffer_desc nameBuffer;
        gss_name_t gssPrincipalName = GSS_C_NO_NAME;
        gss_cred_id_t credential= GSS_C_NO_CREDENTIAL;

        std::string canonicalPrincipalName;
        Status status = canonicalizeServerName(principalName, &canonicalPrincipalName);
        if (!status.isOK())
            goto done;

        nameBuffer.value = const_cast<char*>(canonicalPrincipalName.c_str());
        nameBuffer.length = canonicalPrincipalName.size();

        minorStatus = 0;
        majorStatus = gss_import_name(&minorStatus,
                                      &nameBuffer,
                                      GSS_C_NT_USER_NAME,
                                      &gssPrincipalName);
        if (GSS_ERROR(majorStatus)) {
            status = Status(ErrorCodes::UnknownError,
                            mongoutils::str::stream() << "gssapi could not import name " <<
                            principalName << "; " <<
                            getGssapiErrorString(majorStatus, minorStatus));
            goto done;
        }

        minorStatus = 0;
        majorStatus = gss_acquire_cred(&minorStatus,        // [out] minor_status
                                       gssPrincipalName,    // desired_name
                                       0,                   // time_req
                                       GSS_C_NULL_OID_SET,  // desired_mechs
                                       GSS_C_ACCEPT,        // cred_usage
                                       &credential,         // output_cred_handle
                                       NULL,                // actual_mechs
                                       NULL);               // time_rec
        if (GSS_ERROR(majorStatus)) {
            status = Status(ErrorCodes::UnknownError,
                            mongoutils::str::stream() <<
                            "gssapi could not acquire server credential for " <<
                            canonicalPrincipalName << "; " <<
                            getGssapiErrorString(majorStatus, minorStatus));
            goto done;
        }

        status = Status::OK();
        goto done;

    done:
        if (gssPrincipalName != GSS_C_NO_NAME)
            fassert(4008, !GSS_ERROR(gss_release_name(&minorStatus, &gssPrincipalName)));
        if (credential != GSS_C_NO_CREDENTIAL)
            fassert(4009, !GSS_ERROR(gss_release_cred(&minorStatus, &credential)));

        return status;
    }

}  // namespace gssapi
}  // namespace mongo
