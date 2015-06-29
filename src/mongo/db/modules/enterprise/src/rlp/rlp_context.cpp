/*
 * Copyright (C) 2015 MongoDB, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#include "rlp_context.h"

#include "mongo/util/mongoutils/str.h"
#include "rlp_environment.h"
#include "rlp_loader.h"

namespace mongo {
namespace fts {

RlpContext::~RlpContext() {
    RlpEnvironment* env = _factory->getRlpEnvironment();
    env->BT_RLP_Environment_DestroyContext(env->getEnvironment(), _context);
}

RlpEnvironment* RlpContext::getEnvironment() const {
    return _factory->getRlpEnvironment();
}

void RlpContext::CacheReturnContext::operator()(RlpContext* context) const {
    // Free any temporary buffers in this context
    // The context is still valid to use if you call ProcessBuffer first
    context->_factory->returnContext(context);
}

BT_Result RlpContext::processBuffer(const char* inbuf,
                                    BT_UInt32 inlen,
                                    BT_LanguageID lid,
                                    const char* character_encoding,
                                    const char* mime_type) {
    return _factory->getRlpEnvironment()->BT_RLP_Context_ProcessBuffer(
        _context,
        reinterpret_cast<const unsigned char*>(inbuf),
        inlen,
        lid,
        character_encoding,
        mime_type);
}

void RlpContext::DestoryResultStorage() {
    return _factory->getRlpEnvironment()->BT_RLP_Context_DestroyResultStorage(_context);
}

ContextFactory::ContextCacheUP ContextFactory::getContext(BT_LanguageID language,
                                                          StringData contextXml) {
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);

        std::vector<ContextUP>& contexts = _contextMap[language];

        if (!contexts.empty()) {
            ContextUP context = std::move(contexts.back());
            contexts.pop_back();

            return ContextCacheUP(context.release());
        }
    }

    return ContextCacheUP(createContext(language, contextXml).release());
}

void ContextFactory::returnContext(RlpContext* context) {
    context->DestoryResultStorage();

    stdx::lock_guard<stdx::mutex> lock(_mutex);

    std::vector<ContextUP>& contexts = _contextMap[context->getLanguage()];
    contexts.emplace_back(context);
}

ContextFactory::ContextUP ContextFactory::createContext(BT_LanguageID language,
                                                        StringData contextXml) {
    BT_RLP_ContextC* context;

    BT_Result rc = getRlpEnvironment()->BT_RLP_Environment_GetContextFromBuffer(
        getRlpEnvironment()->getEnvironment(),
        reinterpret_cast<const unsigned char*>(contextXml.rawData()),
        contextXml.size(),
        &context);

    uassert(28631,
            str::stream() << "Unable to create the context for language "
                          << static_cast<int>(language) << " with return code " << rc,
            rc == BT_OK);

    // Note: make_unique is not used to keep RlpContext's constructor private
    ContextUP contextup(new RlpContext(context, language, this));

    getRlpEnvironment()->BT_RLP_Context_SetPropertyValue(
        context, "com.basistech.bl.query", "false");

    return contextup;
}

}  // namespace fts
}  // namespace mongo
