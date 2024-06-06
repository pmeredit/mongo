/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include "mongo/db/keys_collection_document_gen.h"
#include <memory>

namespace mongo {

class ServiceContext;

class TenantToShardCache {
public:
    // Decorate ServiceContext with TenantToShardCache instance.
    static TenantToShardCache* get(ServiceContext* service);
    static void set(ServiceContext* service,
                    std::unique_ptr<TenantToShardCache> tenantToShardCache);

    static void init(ServiceContext* service);

    explicit TenantToShardCache();
};

}  // namespace mongo
