/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <functional>
#include <string>
#include <vector>

#include "mongo/base/string_data.h"

namespace mongo {

/**
 * A segment of a formatted report. This segment will have a Title, and optionally some
 * informational bullet points. An object of this class will execute a series of tests. If any
 * test fails, the segment in the report will conclude with a failure message, and informational
 * bullet points describing the problem and suggesting possible solutions. If all tests pass,
 * the segment in the report will conclude with an affirmative message.
 *
 * Example:
 *
 * Title...
 *     * Informational
 *     * Bullet Points
 * [FAIL] Test failed
 *     * Here's why:
 *     * Advice about how to fix the problem
 *
 */
class Report {
public:
    /**
     * Tracks the assertion failure state for ldap authorization and authentication
     * There are three possible options, which are represented by this enum:
     * kFatalFailure, fatal failure (resulting in program termination), logged with the string
     * '[FAIL]' kNonFatalFailure, non-fatal failure, logged with the string '[FAIL]' kNonFatalInfo,
     * non-fatal failure, logged with the string '[INFO]'
     */
    enum class FailType { kFatalFailure, kNonFatalFailure, kNonFatalInfo };

    class ResultsAssertion {
        using Conditional = std::function<bool()>;

    public:
        ResultsAssertion(Conditional assertedCondition,
                         std::string failureMessage,
                         std::vector<std::string> failureBullets = std::vector<std::string>{},
                         FailType isFatal = FailType::kFatalFailure);

        ResultsAssertion(Conditional assertedCondition,
                         std::string failureMessage,
                         std::function<std::vector<std::string>()> failureBulletGenerator,
                         FailType isFatal = FailType::kFatalFailure);

        Conditional assertedCondition;
        std::string failureMessage;
        std::function<std::vector<std::string>()> failureBulletGenerator;
        FailType isFatal;
    };

    explicit Report(bool color);

    void openSection(StringData testName);

    void printItemList(const std::function<std::vector<std::string>()>& infoBulletGenerator);

    void checkAssert(ResultsAssertion&& assert);

    void closeSection(StringData successMessage);

private:
    [[nodiscard]] StringData okString() const;

    [[nodiscard]] StringData failString() const;

    [[nodiscard]] StringData infoString() const;

    // Tracks whether an error has occurred which should short circuit
    // the remainder of the assert checks for the current section.
    bool _nonFatalAssertTriggered = false;

    bool _color;
};

}  // namespace mongo
