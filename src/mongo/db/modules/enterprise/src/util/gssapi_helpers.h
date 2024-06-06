/*
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */
#pragma once

// `time.h` needs to be included first on some systems, to provide `time_t` for gssapi headers.
#include <ctime>

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

/**
 * RAII guard for objects of type gss_buffer_desc
 */
class GSSBufferDesc {
public:
    GSSBufferDesc() : _buffer(GSS_C_EMPTY_BUFFER) {}
    ~GSSBufferDesc() {
        if (_buffer.length > 0) {
            OM_uint32 minorStatus;
            gss_release_buffer(&minorStatus, &_buffer);
        }
    }
    GSSBufferDesc(const GSSBufferDesc&) = delete;
    GSSBufferDesc& operator=(const GSSBufferDesc&) = delete;
    GSSBufferDesc(GSSBufferDesc&&) = default;
    GSSBufferDesc& operator=(GSSBufferDesc&&) = default;
    gss_buffer_desc* get() {
        return &_buffer;
    }

private:
    gss_buffer_desc _buffer;
};

/**
 * RAII guard for objects of type gss_ctx_id_t
 */
class GSSContextID {
public:
    explicit GSSContextID() : _ctx(GSS_C_NO_CONTEXT) {}
    ~GSSContextID() {
        if (_ctx != GSS_C_NO_CONTEXT) {
            OM_uint32 minorStatus;
            gss_delete_sec_context(&minorStatus, &_ctx, GSS_C_NO_BUFFER);
        }
    }
    GSSContextID(const GSSContextID&) = delete;
    GSSContextID& operator=(const GSSContextID&) = delete;
    GSSContextID(GSSContextID&&) = default;
    GSSContextID& operator=(GSSContextID&&) = default;
    gss_ctx_id_t* get() {
        return &_ctx;
    }

private:
    gss_ctx_id_t _ctx;
};

}  // namespace mongo
