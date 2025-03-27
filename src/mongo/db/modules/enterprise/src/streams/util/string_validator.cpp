/**
 *     Copyright (C) 2025-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "string_validator.h"

#include <initializer_list>
#include <string>

namespace streams {

StringValidator::StringValidator(const std::initializer_list<char>& unsafeChars)
    : _unsafeChars{unsafeChars} {}

void StringValidator::addValidator(std::function<bool(const std::string& str)> validateFunc) {
    _validators.push_back(validateFunc);
}

bool StringValidator::isValid(const std::string& str) {
    // Check if string contains an invalid character.
    for (const char& c : str) {
        if (_unsafeChars.find(c) != _unsafeChars.end()) {
            return false;
        }
    }

    // Check if string passes all validation funcs.
    for (const auto& validationFunc : _validators) {
        if (!validationFunc(str)) {
            return false;
        }
    }

    return true;
}

}  // namespace streams
