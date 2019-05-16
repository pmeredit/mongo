/*
 * Copyright (C) 2019 MongoDB Inc.  All Rights Reserved.
 */
#pragma once

// `time.h` needs to be included first on some systems, to provide `time_t` for gssapi headers.
#include <time.h>

#include <gssapi/gssapi.h>
#include <gssapi/gssapi_krb5.h>

#include <string>

namespace mongo {
/**
 * Return a string describing the GSSAPI error described by "inMajorStatus" and "inMinorStatus."
 */
std::string getGssapiErrorString(OM_uint32 inMajorStatus, OM_uint32 inMinorStatus);

/**
 * RAII guard for objects of type gss_cred_id_t.
 */
class GSSCredId {
public:
    GSSCredId() : _id(GSS_C_NO_CREDENTIAL) {}
    ~GSSCredId();
    GSSCredId(const GSSCredId&) = delete;
    GSSCredId& operator=(const GSSCredId&) = delete;
    GSSCredId(GSSCredId&& other) = default;
    GSSCredId& operator=(GSSCredId&& other) = default;

    gss_cred_id_t* get() {
        return &_id;
    }

private:
    gss_cred_id_t _id;
};


/**
 * RAII guard for objects of type gss_name_t.
 */
class GSSName {
public:
    GSSName() : _name(GSS_C_NO_NAME) {}
    ~GSSName();
    GSSName(const GSSName&) = delete;
    GSSName& operator=(const GSSName&) = delete;
    GSSName(GSSName&& other) = default;
    GSSName& operator=(GSSName&& other) = default;

    gss_name_t* get() {
        return &_name;
    }

private:
    gss_name_t _name;
};


}  // namespace mongo
