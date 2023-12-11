/**
 * Copyright (C) 2023 MongoDB, Inc.  All Rights Reserved.
 */

#include "magic_restore/magic_restore.h"
#include "magic_restore/magic_restore_structs_gen.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/unittest/assert.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"
#include <fstream>
#include <iostream>
#include <sstream>

namespace mongo {
namespace repl {

// Used to mock an input stream for BSONStreamReader. Handles writing BSON object raw data into the
// stream.
class MockStream {
public:
    MockStream() = default;

    MockStream& operator<<(const BSONObj& obj) {
        _stream.write(obj.objdata(), obj.objsize());
        return *this;
    }

    // BSONStreamReader needs access to the underlying std::istream.
    std::stringstream& stream() {
        return _stream;
    }

    void setStreamState(std::ios::iostate state) {
        _stream.setstate(state);
    }

private:
    // std::stringstream derives from std::istream, and is easier to write test data into than an
    // input-specific stream.
    std::stringstream _stream;
};


TEST(MagicRestore, BSONStreamReaderReadOne) {
    auto mock = MockStream();
    auto obj = BSON("_id" << 1);
    mock << obj;

    auto reader = mongo::magic_restore::BSONStreamReader(mock.stream());
    ASSERT(reader.hasNext());
    auto bsonObj = reader.getNext();
    ASSERT_BSONOBJ_EQ(obj, bsonObj);

    ASSERT_EQUALS(reader.getTotalBytesRead(), obj.objsize());
    ASSERT_EQUALS(reader.getTotalObjectsRead(), 1);
    mock.setStreamState(std::ios::eofbit);
    ASSERT(!reader.hasNext());
}

TEST(MagicRestore, BSONStreamReaderReadMultiple) {
    auto obj1 = BSON("_id" << 1);
    auto obj2 = BSON("_id" << 2 << "array" << BSON_ARRAY(BSON("_id" << 3) << BSON("_id" << 4)));
    auto obj3 = BSON("_id" << 5 << "key"
                           << "value");

    auto mock = MockStream();
    // Multiple items in the stream are parsed one by one.
    mock << obj1;
    mock << obj2;

    auto reader = mongo::magic_restore::BSONStreamReader(mock.stream());

    ASSERT(reader.hasNext());
    ASSERT_BSONOBJ_EQ(obj1, reader.getNext());
    ASSERT(reader.hasNext());
    ASSERT_BSONOBJ_EQ(obj2, reader.getNext());

    // An item that is sent to the stream after the BSON reader began reading is parsed correctly.
    mock << obj3;
    ASSERT(reader.hasNext());
    ASSERT_BSONOBJ_EQ(obj3, reader.getNext());

    ASSERT_EQUALS(reader.getTotalBytesRead(), obj1.objsize() + obj2.objsize() + obj3.objsize());
    ASSERT_EQUALS(reader.getTotalObjectsRead(), 3);
    mock.setStreamState(std::ios::eofbit);
    ASSERT(!reader.hasNext());
}

TEST(MagicRestore, BSONStreamReaderEmptyBSON) {
    auto mock = MockStream();
    auto obj = BSONObj();
    mock << obj;
    auto reader = mongo::magic_restore::BSONStreamReader(mock.stream());

    // Empty BSON objects should parse correctly.
    auto readBson = reader.getNext();
    ASSERT_BSONOBJ_EQ(obj, readBson);

    ASSERT_EQUALS(reader.getTotalBytesRead(), obj.objsize());
    ASSERT_EQUALS(reader.getTotalObjectsRead(), 1);
    // Empty BSON objects cannot be parsed into a valid oplog entry.
    ASSERT_THROWS_CODE(OplogEntry(readBson), mongo::DBException, ErrorCodes::IDLFailedToParse);
}

DEATH_TEST(MagicRestore, BSONStreamReaderNegativeSize, "Parsed invalid BSON length") {
    // Insert a negative BSON size by inserting -11 into the first four bytes of the stream.
    char negativeSize[4] = {static_cast<char>((-11 >> 0) & 0xFF),
                            static_cast<char>((-11 >> 8) & 0xFF),
                            static_cast<char>((-11 >> 16) & 0xFF),
                            static_cast<char>((-11 >> 24) & 0xFF)};

    std::stringstream stream(std::string(negativeSize, 4));
    auto reader = mongo::magic_restore::BSONStreamReader(stream);
    auto obj = reader.getNext();
}

DEATH_TEST(MagicRestore, BSONStreamReaderSmallSize, "Parsed invalid BSON length") {
    // Insert a BSON size smaller than the min document size by inserting 4 into the first four
    // bytes of the stream.
    char smallSize[4] = {static_cast<char>((4 >> 0) & 0xFF),
                         static_cast<char>((4 >> 8) & 0xFF),
                         static_cast<char>((4 >> 16) & 0xFF),
                         static_cast<char>((4 >> 24) & 0xFF)};

    std::stringstream stream(std::string(smallSize, 4));
    auto reader = mongo::magic_restore::BSONStreamReader(stream);
    auto obj = reader.getNext();
}

DEATH_TEST(MagicRestore, BSONStreamReaderSizeTooLarge, "Parsed invalid BSON length") {
    // Insert a BSON size greater than the max document size by inserting 16777217 into the first
    // four bytes of the stream.
    char bigSize[4] = {static_cast<char>((16777217 >> 0) & 0xFF),
                       static_cast<char>((16777217 >> 8) & 0xFF),
                       static_cast<char>((16777217 >> 16) & 0xFF),
                       static_cast<char>((16777217 >> 24) & 0xFF)};

    std::stringstream stream(std::string(bigSize, 4));
    auto reader = mongo::magic_restore::BSONStreamReader(stream);
    auto obj = reader.getNext();
}

DEATH_TEST(MagicRestore, BSONStreamReaderStreamFailbit, "Failed to read BSON length") {
    auto mock = MockStream();
    auto obj1 = BSON("_id" << 1);
    mock << obj1;

    auto reader = mongo::magic_restore::BSONStreamReader(mock.stream());
    // With the failbit set, `getNext()` should fassert.
    mock.setStreamState(std::ios::failbit);
    auto obj = reader.getNext();
}

DEATH_TEST(MagicRestore,
           BSONStreamReaderReadLessThanSizeBytes,
           "Failed to read entire BSON object") {
    // Specifying a BSON size of 6 but only including 5 bytes will fail.
    char size[5] = {static_cast<char>((6 >> 0) & 0xFF),
                    static_cast<char>((6 >> 8) & 0xFF),
                    static_cast<char>((6 >> 16) & 0xFF),
                    static_cast<char>((6 >> 24) & 0xFF),
                    static_cast<char>(1)};

    std::stringstream stream(std::string(size, 5));
    auto reader = mongo::magic_restore::BSONStreamReader(stream);
    auto obj = reader.getNext();
}

// This test uses a binary file containing oplog entries generated by Cloud. The file is well-formed
// raw BSON data.
TEST(MagicRestore, BSONStreamReaderReadSampleCloudOplogs) {
    boost::filesystem::path filePath(boost::filesystem::current_path() / "src" / "mongo" / "db" /
                                     "modules" / "enterprise" / "src" / "magic_restore" /
                                     "bson_reader_test_data" / "sample_cloud_oplogs");

    std::ifstream stream(filePath.c_str(), std::ios::binary);
    ASSERT(stream.is_open());

    auto reader = mongo::magic_restore::BSONStreamReader(stream);
    ASSERT(reader.hasNext());
    while (reader.hasNext()) {
        auto bsonObj = reader.getNext();
        // Ensure the parsed BSON object is an oplog entry.
        ASSERT_DOES_NOT_THROW(OplogEntry o(bsonObj));
    }
    // The Cloud file has 12 oplog entries in it.
    ASSERT_EQUALS(reader.getTotalBytesRead(), boost::filesystem::file_size(filePath));
    ASSERT_EQUALS(reader.getTotalObjectsRead(), 12);
}

}  // namespace repl
}  // namespace mongo
