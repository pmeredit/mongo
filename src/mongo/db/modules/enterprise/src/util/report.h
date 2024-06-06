/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
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
