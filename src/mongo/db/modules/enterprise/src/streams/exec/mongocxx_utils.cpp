/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/mongocxx_utils.h"
#include "mongo/db/json.h"
#include "mongo/db/service_context.h"
#include "mongo/platform/basic.h"

#include <bsoncxx/json.hpp>

namespace streams {

using namespace mongo;

namespace {

static const auto _decoration =
    ServiceContext::declareDecoration<std::unique_ptr<mongocxx::instance>>();

}

mongocxx::instance* getMongocxxInstance(ServiceContext* svcCtx) {
    auto& mongocxxInstance = _decoration(svcCtx);
    static std::once_flag initOnce;
    std::call_once(initOnce, [&]() {
        dassert(!mongocxxInstance);
        mongocxxInstance = std::make_unique<mongocxx::instance>();
    });
    return mongocxxInstance.get();
}

// TODO: Current implementation is quite inefficient as we convert to json first.
bsoncxx::document::value toBsoncxxDocument(const BSONObj& obj) {
    return bsoncxx::from_json(tojson(obj));
}

}  // namespace streams
