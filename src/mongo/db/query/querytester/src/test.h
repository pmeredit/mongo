/**
 *    Copyright (C) 2024-present MongoDB, Inc.
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

#include <filesystem>
#include <fstream>
#include <string>

#include "file_helpers.h"
#include "mongo/client/dbclient_connection.h"
#include "mongo/db/query/util/jparse_util.h"
#include "mongo/shell/shell_utils.h"

namespace queryTester {
using mongo::shell_utils::NormalizationOpts;
using mongo::shell_utils::NormalizationOptsSet;

enum class ModeOption { Run, Compare, Normalize };
ModeOption stringToModeOption(const std::string&);

class Test {
public:
    Test(const std::string& testLine,
         const size_t testNum,
         boost::optional<std::string> testName,
         std::vector<std::string>&& preTestComments,
         std::vector<std::string>&& preQueryComments,
         std::vector<std::string>&& postQueryComments,
         std::vector<std::string>&& postTestComments,
         std::vector<mongo::BSONObj>&& expectedResult = {})
        : _testLine(testLine),
          _testNum(testNum),
          _testName(testName),
          _comments({preTestComments, preQueryComments, postQueryComments, postTestComments}),
          _expectedResult(std::move(expectedResult)) {
        parseTestQueryLine();
    }

    /**
     * Parses a single test definition. This includes the number and name line, the comment line(s)
     * and the actual test command.
     * Expects the file stream to be open and allow reading.
    <----- Test Format ----->
    <testNumber> <testName>
    Comments on the test, any number of lines
    <testMode> {commandObj}
    <result if result file>
    <----- End Test Format ----->
     */
    static Test parseTest(std::fstream&, ModeOption, size_t testNum);

    /**
     * Compute the normalized version of the input set.
     */
    static std::vector<std::string> normalize(const std::vector<mongo::BSONObj>&,
                                              NormalizationOptsSet);

    /**
     * Returns all actual results, including those that are not contained in the first batch.
     */
    std::vector<mongo::BSONObj> getAllResults(mongo::DBClientConnection* conn,
                                              const mongo::BSONObj& result);

    /**
     * Runs the test and records the result returned by the server.
     */
    void runTestAndRecord(mongo::DBClientConnection*, ModeOption);

    std::string testName;

    static NormalizationOptsSet parseResultType(const std::string& type) {
        static const std::map<std::string, NormalizationOptsSet> typeMap = {
            {":normalizeFull",
             NormalizationOpts::kSortResults | NormalizationOpts::kSortBSON |
                 NormalizationOpts::kSortArrays | NormalizationOpts::kNormalizeNumerics |
                 NormalizationOpts::kConflateNullAndMissing},
            {":normalizeNonNull",
             NormalizationOpts::kSortResults | NormalizationOpts::kSortBSON |
                 NormalizationOpts::kSortArrays | NormalizationOpts::kNormalizeNumerics},
            {":sortFull",
             NormalizationOpts::kSortResults | NormalizationOpts::kSortBSON |
                 NormalizationOpts::kSortArrays},
            {":sortBSONNormalizeNumerics",
             NormalizationOpts::kSortResults | NormalizationOpts::kSortBSON |
                 NormalizationOpts::kNormalizeNumerics},
            {":sortBSON", NormalizationOpts::kSortResults | NormalizationOpts::kSortBSON},
            {":sortResultsNormalizeNumerics",
             NormalizationOpts::kSortResults | NormalizationOpts::kNormalizeNumerics},
            {":normalizeNumerics", NormalizationOpts::kNormalizeNumerics},
            {":normalizeNulls", NormalizationOpts::kConflateNullAndMissing},
            {":sortResults", NormalizationOpts::kSortResults},
            {":results", NormalizationOpts::kResults}};

        if (auto it = typeMap.find(type); it != typeMap.end()) {
            return it->second;
        } else {
            uasserted(9670456, mongo::str::stream() << "Unexpected test type " << type);
        }
    }

    auto getTestLine() const {
        return _testLine;
    }

    size_t getTestNum() const {
        return _testNum;
    }

    void setDB(const std::string& db) {
        _db = db;
    }

    void writeToStream(std::fstream& fs, WriteOutOptions resultOpt = WriteOutOptions::kNone) const;

private:
    void parseTestQueryLine();
    std::string _testLine;
    size_t _testNum;
    boost::optional<std::string> _testName;
    struct {
        std::vector<std::string> preTest;
        std::vector<std::string> preQuery;
        std::vector<std::string> postQuery;
        std::vector<std::string> postTest;
    } _comments;
    std::vector<mongo::BSONObj> _expectedResult;
    std::vector<std::string> _normalizedResult;
    NormalizationOptsSet _testType;
    mongo::BSONObj _query;
    std::string _db;
};

}  // namespace queryTester
