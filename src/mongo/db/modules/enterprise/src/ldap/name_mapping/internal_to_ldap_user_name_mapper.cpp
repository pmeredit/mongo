/**
 *  Copyright (C) 2016-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include "mongo/platform/basic.h"

#include "internal_to_ldap_user_name_mapper.h"

#include <memory>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/json.h"
#include "mongo/db/auth/user_acquisition_stats.h"
#include "mongo/logv2/log.h"
#include "mongo/util/str.h"

#include "ldap/ldap_parameters_gen.h"
#include "ldap/ldap_runner.h"
#include "ldap_rewrite_rule.h"
#include "regex_rewrite_rule.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kAccessControl


namespace mongo {

InternalToLDAPUserNameMapper::InternalToLDAPUserNameMapper(
    std::vector<std::unique_ptr<RewriteRule>> rules, std::string userToDNMapping)
    : _transformations(std::move(rules)), _userToDNMapping(std::move(userToDNMapping)) {}

InternalToLDAPUserNameMapper::InternalToLDAPUserNameMapper(InternalToLDAPUserNameMapper&& other) =
    default;
InternalToLDAPUserNameMapper& InternalToLDAPUserNameMapper::operator=(
    InternalToLDAPUserNameMapper&& other) = default;

StatusWith<std::string> InternalToLDAPUserNameMapper::transform(
    LDAPRunner* runner,
    StringData input,
    TickSource* tickSource,
    const SharedUserAcquisitionStats& userAcquisitionStats) const {
    if (0 == _transformations.size()) {
        LOGV2_DEBUG(5264500, 3, "Using LDAP username as is", "user"_attr = input);
        return input.toString();
    }

    StringBuilder errorStack;
    for (const auto& transform : _transformations) {
        StatusWith<std::string> result =
            transform->resolve(runner, input, tickSource, userAcquisitionStats);

        if (result.isOK()) {
            LOGV2_DEBUG(
                24065, 3, "Transformed username to: {user}", "user"_attr = result.getValue());
            return result;
        } else {
            LOGV2_DEBUG(4478600,
                        3,
                        "Transformation failed",
                        "rule"_attr = transform->toStringData(),
                        "error"_attr = result.getStatus());
        }

        auto status = result.getStatus();
        errorStack << "{ rule: " << transform->toStringData() << " error: \"" << status.toString()
                   << "\" }, ";

        if (ldapAbortOnNameMappingFailure && (status.code() == ErrorCodes::OperationFailed)) {
            // OperationFailed indicates we tried to talk to LDAP,
            // but the server was unreachable/unauthenticated/etc...
            // It does not necessarily mean the rule would not have matched.
            return {status.code(),
                    str::stream() << "Username mapping operation failed, aborting transformation. "
                                  << errorStack.str()};
        }
    }

    return Status(ErrorCodes::FailedToParse,
                  str::stream() << "Failed to transform user '" << input
                                << "'. No matching transformation out of "
                                << _transformations.size()
                                << " available transformations. Results: " << errorStack.str());
}

StatusWith<InternalToLDAPUserNameMapper> InternalToLDAPUserNameMapper::createNameMapper(
    std::string userToDNMapping) {
    BSONObj config;
    try {
        config = fromjson(userToDNMapping);
    } catch (const DBException&) {
        return Status(ErrorCodes::FailedToParse,
                      "Failed to parse JSON description of the relationship between "
                      "MongoDB usernames and LDAP DNs");
    }

    std::vector<std::unique_ptr<RewriteRule>> transforms;

    if (!config.couldBeArray()) {
        // If a single object is received, attempt to convert to BSON array.
        BSONArrayBuilder arr;
        arr.append(config);
        config = arr.arr();
    }

    for (const BSONElement& element : config) {
        if (element.type() != BSONType::Object) {
            return Status{ErrorCodes::FailedToParse,
                          str::stream()
                              << "InternalToLDAPUserNameMapper::createNameMapper expects "
                                 "an array of BSON objects, but observed an object of type "
                              << element.type()};
        }

        BSONObj obj = element.Obj();
        BSONElement match = obj["match"];
        BSONElement substitution = obj["substitution"];
        BSONElement ldapQuery = obj["ldapQuery"];

        if (!match.ok()) {
            return Status{ErrorCodes::FailedToParse,
                          "InternalToLDAPUserNameMapper::createNameMapper expects objects with a "
                          "\"match\" element."};
        } else if (match.type() != BSONType::String) {
            return Status{
                ErrorCodes::FailedToParse,
                "InternalToLDAPUserNameMapper::createNameMapper expects \"match\" elements "
                "to be strings."};
        }

        enum class RuleType { Invalid, Regex, LDAP };

        RuleType ruleType = RuleType::Invalid;
        if (substitution.ok() && !ldapQuery.ok())
            ruleType = RuleType::Regex;
        else if (!substitution.ok() && ldapQuery.ok())
            ruleType = RuleType::LDAP;

        switch (ruleType) {
            case RuleType::Regex: {
                if (substitution.type() != BSONType::String) {
                    return Status{ErrorCodes::FailedToParse,
                                  "InternalToLDAPUserNameMapper::createNameMapper requires that "
                                  "\"substitution\" elements to be strings."};
                }

                StatusWith<RegexRewriteRule> swRegexRewriteRule =
                    RegexRewriteRule::create(match.str(), substitution.str());
                if (!swRegexRewriteRule.isOK()) {
                    return {swRegexRewriteRule.getStatus()};
                }

                transforms.emplace_back(
                    std::make_unique<RegexRewriteRule>(std::move(swRegexRewriteRule.getValue())));
                break;
            }
            case RuleType::LDAP: {
                if (ldapQuery.type() != BSONType::String) {
                    return Status{ErrorCodes::FailedToParse,
                                  "InternalToLDAPUserNameMapper::createNameMapper requires that "
                                  "\"ldapQuery\" elements to be strings."};
                }

                StatusWith<LDAPRewriteRule> swLDAPRewriteRule =
                    LDAPRewriteRule::create(match.str(), ldapQuery.str());
                if (!swLDAPRewriteRule.isOK()) {
                    return {swLDAPRewriteRule.getStatus()};
                }

                transforms.emplace_back(
                    std::make_unique<LDAPRewriteRule>(std::move(swLDAPRewriteRule.getValue())));
                break;
            }
            case RuleType::Invalid:
                return Status{
                    ErrorCodes::FailedToParse,
                    "InternalToLDAPUserNameMapper::createNameMapper requires elements contain "
                    "one element name \"match\", and exactly one element named either "
                    "\"substitution\" or \"ldapQuery\"."};
        }
    }

    return StatusWith<InternalToLDAPUserNameMapper>{
        InternalToLDAPUserNameMapper(std::move(transforms), std::move(userToDNMapping))};
}
}  // namespace mongo
