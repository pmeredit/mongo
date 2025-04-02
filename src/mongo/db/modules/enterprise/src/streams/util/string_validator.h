/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <functional>
#include <initializer_list>
#include <string>
#include <unordered_set>

namespace streams {

/**
 * This class performs string validation on an input string. Additional validation functions can be
 * passed-in to impose additional string validation.
 */
class StringValidator {
public:
    StringValidator(const std::initializer_list<char>& unsafeChars);

    void addValidator(std::function<bool(const std::string& str)> validateFunc);

    bool isValid(const std::string& str);

private:
    std::unordered_set<char> _unsafeChars;
    std::vector<std::function<bool(const std::string& str)>> _validators;
};

}  // namespace streams
