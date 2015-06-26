/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#pragma once

#include "ldap_runner_interface.h"

#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

/**
 * Mock implementation of the LDAPRunnerInterface. Consumers must load instantiations with
 * queries and their results using the ``push'' method, which stores them on a stack. runQuery will
 * pop the stack, assert its query matches the caller's, and return the stored result.
 */
class MockLDAPRunner : public LDAPRunnerInterface {
public:
    ~MockLDAPRunner() final = default;
    StatusWith<LDAPEntityCollection> runQuery(const LDAPQuery& query) final {
        ASSERT_FALSE(_stored.empty());
        auto next = std::move(_stored.back());
        _stored.pop_back();
        ASSERT_EQ(next.query, query);
        return next.results;
    };

    void push(std::string queryString, StatusWith<LDAPEntityCollection> entities) {
        auto swQueryParameters = LDAPQueryConfig::createLDAPQueryConfig(queryString);
        ASSERT_OK(swQueryParameters.getStatus());
        std::unique_ptr<LDAPQueryConfig> parameters =
            stdx::make_unique<LDAPQueryConfig>(std::move(swQueryParameters.getValue()));

        auto swLDAPQuery = LDAPQuery::instantiateQuery(*parameters);
        ASSERT_OK(swLDAPQuery.getStatus());

        MockQueryEntry savedQuery{
            std::move(swLDAPQuery.getValue()), std::move(parameters), std::move(entities)};

        _stored.emplace_back(std::move(savedQuery));
    }

private:
    // The MockQueryEntry cannot be constructed piece by piece, because LDAPQuery doesn't have a
    // default constructor. But, LDAPQuery must be constructed with a reference to the final
    // LDAPQueryConfig object, after it has been moved or copied into its final resting place.
    // So, LDAPQueryConfig must be created, then moved onto the heap. LDAPQuery can be
    // constructed with a reference to the heap object and placed into the Entry.
    struct MockQueryEntry {
        LDAPQuery query;
        std::unique_ptr<LDAPQueryConfig> parameters;
        StatusWith<LDAPEntityCollection> results;
    };
    std::vector<MockQueryEntry> _stored;
};
}  // namespace mongo
