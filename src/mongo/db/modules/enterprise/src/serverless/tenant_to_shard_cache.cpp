/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "tenant_to_shard_cache.h"
#include "mongo/base/init.h"
#include "mongo/db/service_context.h"
#include "mongo/platform/basic.h"
#include "mongo/s/initialize_tenant_to_shard_cache.h"

namespace mongo {

namespace {
Mutex tenantToShardCacheMutex;  // protects access to decoration instance of TenantToShardCache.
}

const auto getTenantToShardCache =
    ServiceContext::declareDecoration<std::unique_ptr<TenantToShardCache>>();

TenantToShardCache* TenantToShardCache::get(ServiceContext* service) {
    stdx::lock_guard<Latch> lk(tenantToShardCacheMutex);
    return getTenantToShardCache(service).get();
}

void TenantToShardCache::set(ServiceContext* service,
                             std::unique_ptr<TenantToShardCache> newTenantToShardCache) {
    stdx::lock_guard<Latch> lk(tenantToShardCacheMutex);
    auto& tenantToShardCache = getTenantToShardCache(service);
    tenantToShardCache = std::move(newTenantToShardCache);
}

TenantToShardCache::TenantToShardCache() {}

void TenantToShardCache::init(ServiceContext* service) {
    TenantToShardCache::set(service, std::make_unique<TenantToShardCache>());
}

MONGO_INITIALIZER(InitializeTenantToShardCache)(InitializerContext*) {
    registerTenantToShardCacheInitializer(&TenantToShardCache::init);
}

}  // namespace mongo
