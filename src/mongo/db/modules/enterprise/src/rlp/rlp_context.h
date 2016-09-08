/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <bt_rlp_c.h>
#include <memory>
#include <vector>

#include "mongo/base/disallow_copying.h"
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace fts {
class ContextFactory;
class RlpEnvironment;

/**
 * RlpContext
 *
 * Holds a BT_RLP_ContextC. Non-ContextFactory holders should always pair this with
 * CacheReturnContext in a unique_ptr.
 *
 * Single-threaded
 *
 * Also contains enough information about a language for a token iterator to process
 * a given language.
 */
class RlpContext {
    MONGO_DISALLOW_COPYING(RlpContext);
    friend class ContextFactory;

public:
    ~RlpContext();

    /**
     * Processes a string according to BT_LanguageID, and current context
     * Invalidates an existing iterator.
     *
     * See BT_RLP_Context_ProcessBuffer in
     *  rlp/doc/api-reference/c-reference/bt__rlp__c_8h.html
     */
    BT_Result processBuffer(const char* inbuf,
                            BT_UInt32 inlen,
                            BT_LanguageID lid,
                            const char* character_encoding,
                            const char* mime_type);
    /**
     * This member frees any storage allocated for results in the context.
     */
    void DestoryResultStorage();

    BT_RLP_ContextC* getContext() const {
        return _context;
    }
    BT_LanguageID getLanguage() const {
        return _language;
    }
    RlpEnvironment* getEnvironment() const;

public:
    /**
     * CacheReturnContext
     *
     * std::unique_ptr Deleter function object to return RlpContext to ContextFactory
     */
    class CacheReturnContext {
    public:
        void operator()(RlpContext* context) const;
    };

private:
    RlpContext(BT_RLP_ContextC* context, BT_LanguageID language, ContextFactory* factory)
        : _context(context), _language(language), _factory(factory) {}

private:
    BT_RLP_ContextC* _context;
    BT_LanguageID _language;
    ContextFactory* _factory;
};

/**
 * BTLanguageHash
 *
 * Hash Function for BT_LanguageID
 */
class BTLanguageHash {
public:
    std::size_t operator()(BT_LanguageID const& s) const {
        return s;
    }
};

/**
 * ContextFactory
 *
 * Thread-safe factory to create new Contexts and cache existing contexts
 */
class ContextFactory {
    MONGO_DISALLOW_COPYING(ContextFactory);

    typedef std::unique_ptr<RlpContext> ContextUP;
    typedef std::unique_ptr<RlpContext, RlpContext::CacheReturnContext> ContextCacheUP;

public:
    ContextFactory(RlpEnvironment* rlpEnvironment) : _rlpEnvironment(rlpEnvironment) {}

    /**
     * Get a RLP Context from the factory for specified language with context
     */
    ContextCacheUP getContext(BT_LanguageID language, StringData contextXml);

    /**
     * Return a context back to the factory, used by RlpContext::CacheReturnContext
     */
    void returnContext(RlpContext* context);

    /**
     *  Get the RlpEnvironment
     */
    RlpEnvironment* getRlpEnvironment() {
        return _rlpEnvironment;
    }

private:
    /**
     * Creates a new RLP Context for the specified language and context
     */
    ContextUP createContext(BT_LanguageID language, StringData contextXml);

private:
    RlpEnvironment* const _rlpEnvironment;

    stdx::mutex _mutex;
    stdx::unordered_map<BT_LanguageID, std::vector<ContextUP>, BTLanguageHash> _contextMap;
};

}  // namespace fts
}  // namespace mongo
