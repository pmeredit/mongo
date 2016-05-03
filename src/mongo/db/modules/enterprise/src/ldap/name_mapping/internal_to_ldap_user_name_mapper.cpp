/**
 *  Copyright (C) 2016 MongoDB Inc.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kAccessControl

#include "mongo/platform/basic.h"

#include "internal_to_ldap_user_name_mapper.h"

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/operation_context.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include "ldap_rewrite_rule.h"
#include "regex_rewrite_rule.h"

namespace mongo {

InternalToLDAPUserNameMapper::InternalToLDAPUserNameMapper(
    std::vector<std::unique_ptr<RewriteRule>> rules)
    : _transformations(std::move(rules)) {}

InternalToLDAPUserNameMapper::InternalToLDAPUserNameMapper(InternalToLDAPUserNameMapper&& other) =
    default;
InternalToLDAPUserNameMapper& InternalToLDAPUserNameMapper::operator=(
    InternalToLDAPUserNameMapper&& other) = default;

StatusWith<std::string> InternalToLDAPUserNameMapper::transform(OperationContext* txn,
                                                                StringData input) const {
    for (const auto& transform : _transformations) {
        StatusWith<std::string> result = transform->resolve(txn, input);
        LOG(3) << "Transforming username: " << input << " using rule: " << transform->toStringData()
               << ". Result: " << [](const StatusWith<std::string>& result) {
                   return result.isOK() ? std::string("PASS. New userName is ") + result.getValue()
                                        : "FAILED. Attempting next rewrite rule";
               }(result);
        if (result.isOK()) {
            return result;
        }
    }

    return Status(
        ErrorCodes::FailedToParse,
        str::stream() << "Failed to transform username. No matching transformation out of "
                      << _transformations.size() << " available transformations.");
}

StatusWith<InternalToLDAPUserNameMapper> InternalToLDAPUserNameMapper::createNameMapper(
    BSONObj config) {
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
                    stdx::make_unique<RegexRewriteRule>(std::move(swRegexRewriteRule.getValue())));
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
                    stdx::make_unique<LDAPRewriteRule>(std::move(swLDAPRewriteRule.getValue())));
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
        InternalToLDAPUserNameMapper(std::move(transforms))};
}
}  // namespace mongo
