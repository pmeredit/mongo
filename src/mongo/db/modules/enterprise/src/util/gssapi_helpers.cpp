/*
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "gssapi_helpers.h"

#include "mongo/base/string_data.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"


namespace mongo {
namespace {
/**
 * Write what the GSSAPI library considers to be the user-displayable description of "inStatus"
 * to "os".
 *
 * "statusType" indicates whether "inStatus" is a major status code (GSS_C_GSS_CODE) or minor
 * status code (GSS_C_MECH_CODE).  When it is a minor code, this function always assumes that
 * the mechanism is gss_mech_krb5.
 */
void putGssapiStatusString(str::stream* os, OM_uint32 inStatus, int statusType) {
    OM_uint32 majorStatus;
    OM_uint32 minorStatus;
    OM_uint32 context = 0;
    gss_buffer_desc message = {0};

    if (statusType == GSS_C_GSS_CODE) {
        *os << "Major code " << inStatus << "; ";
    } else if (statusType == GSS_C_MECH_CODE) {
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
            *os << (message.length > 0
                        ? StringData(static_cast<const char*>(message.value), message.length)
                        : "(no data)"_sd)
                << "; ";
            fassert(4010, !GSS_ERROR(gss_release_buffer(&minorStatus, &message)));
        } else {
            *os << "gss_display_status() failed on context value " << context << "; ";
            context = 0;
        }
    } while (context != 0);
}

}  // namespace

GSSCredId::~GSSCredId() {
    OM_uint32 minorStatus;
    OM_uint32 majorStatus = gss_release_cred(&minorStatus, &_id);
    fassert(51200, majorStatus == GSS_S_COMPLETE);
}

GSSName::~GSSName() {
    OM_uint32 minorStatus;
    OM_uint32 majorStatus = gss_release_name(&minorStatus, &_name);
    fassert(51228, majorStatus == GSS_S_COMPLETE);
}

}  // namespace mongo

/**
 * Return a string describing the GSSAPI error described by "inMajorStatus" and "inMinorStatus."
 */
std::string mongo::getGssapiErrorString(OM_uint32 inMajorStatus, OM_uint32 inMinorStatus) {
    mongo::str::stream os;
    mongo::putGssapiStatusString(&os, inMajorStatus, GSS_C_GSS_CODE);
    mongo::putGssapiStatusString(&os, inMinorStatus, GSS_C_MECH_CODE);
    return os;
}
