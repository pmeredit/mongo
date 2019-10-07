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

#include "mongo/platform/basic.h"

#include "report.h"

#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include "mongo/base/string_data.h"
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
        quickExit(-1);
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
