/**
 * Copyright (C) 2019-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "mongo/platform/basic.h"

#include "report.h"

#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/util/exit_code.h"
#include "mongo/util/quick_exit.h"

namespace mongo {

Report::Report(bool color) : _color(color) {}

void Report::openSection(StringData testName) {
    _nonFatalAssertTriggered = false;
    std::cout << testName << "..." << std::endl;
}

void Report::printItemList(const std::function<std::vector<std::string>()>& infoBulletGenerator) {
    if (_nonFatalAssertTriggered) {
        return;
    }
    auto infoBullets = infoBulletGenerator();
    for (const std::string& infoBullet : infoBullets) {
        std::cout << "\t* " << infoBullet << std::endl;
    }
}

void Report::checkAssert(Report::ResultsAssertion&& assert) {
    if (_nonFatalAssertTriggered || assert.assertedCondition()) {
        return;
    }

    StringData errorString =
        (assert.isFatal == FailType::kNonFatalInfo) ? infoString() : failString();

    std::cout << errorString << assert.failureMessage << std::endl;
    auto failureBullets = assert.failureBulletGenerator();
    for (const std::string& bullet : failureBullets) {
        std::cout << "\t* " << bullet << std::endl;
    }
    std::cout << std::endl;

    if (assert.isFatal == FailType::kFatalFailure) {
        // Fatal assertion failures are logged and the test program terminates immediately
        quickExit(ExitCode::fail);
    } else {
        // Non fatal failures will prevent the rest of the section from being executed
        _nonFatalAssertTriggered = true;
    }
}

StringData Report::okString() const {
    if (_color) {
        return "[\x1b[32mOK\x1b[0m] ";
    }
    return "[OK] ";
}

StringData Report::failString() const {
    if (_color) {
        return "[\x1b[31mFAIL\x1b[0m] ";
    }
    return "[FAIL] ";
}

StringData Report::infoString() const {
    if (_color) {
        return "[\x1b[33mINFO\x1b[0m] ";
    }
    return "[INFO] ";
}
void Report::closeSection(StringData successMessage) {
    if (!_nonFatalAssertTriggered) {
        std::cout << okString() << successMessage << std::endl << std::endl;
    }
}

Report::ResultsAssertion::ResultsAssertion(
    Conditional assertedCondition,
    std::string failureMessage,
    std::function<std::vector<std::string>()> failureBulletGenerator,
    FailType isFatal)
    : assertedCondition(std::move(assertedCondition)),
      failureMessage(std::move(failureMessage)),
      failureBulletGenerator(std::move(failureBulletGenerator)),
      isFatal(isFatal) {}

Report::ResultsAssertion::ResultsAssertion(Conditional assertedCondition,
                                           std::string failureMessage,
                                           std::vector<std::string> failureBullets,
                                           FailType isFatal)
    : assertedCondition(std::move(assertedCondition)),
      failureMessage(std::move(failureMessage)),
      failureBulletGenerator(
          [failureBullets{std::move(failureBullets)}] { return failureBullets; }),
      isFatal(isFatal) {}

}  // namespace mongo
