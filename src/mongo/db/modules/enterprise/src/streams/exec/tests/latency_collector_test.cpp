/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/latency_collector.h"

#include "mongo/unittest/unittest.h"

namespace streams {

TEST(LatencyCollectorTest, Basic) {
    LoggingContext loggingContext{"id", "name", "tenant"};
    LatencyCollector collector(loggingContext);
    collector.add(LatencyCollector::LatencyInfo{.sourceTime = Milliseconds(1),
                                                .readTime = Milliseconds(2),
                                                .writeTime = Milliseconds(3),
                                                .commitTime = Milliseconds(4)});
    LatencyCollector::MaxLatencyDeltas ex{Milliseconds(1),
                                          Milliseconds(1),
                                          Milliseconds(1),
                                          Milliseconds(1),
                                          Milliseconds(1),
                                          Milliseconds(1),
                                          Milliseconds(3),
                                          Milliseconds(1)};
    ASSERT_EQ(collector.getMax(), ex);
    collector.add(LatencyCollector::LatencyInfo{.sourceTime = Milliseconds(20),
                                                .readTime = Milliseconds(30),
                                                .writeTime = Milliseconds(40),
                                                .commitTime = Milliseconds(50)});
    ex = {Milliseconds(10),
          Milliseconds(20),
          Milliseconds(10),
          Milliseconds(20),
          Milliseconds(10),
          Milliseconds(20),
          Milliseconds(30),
          Milliseconds(20)};
    ASSERT_EQ(collector.getMax(), ex);
    collector.add(LatencyCollector::LatencyInfo{.sourceTime = Milliseconds(100),
                                                .readTime = Milliseconds(102),
                                                .writeTime = Milliseconds(198),
                                                .commitTime = Milliseconds(300)});
    ex = {Milliseconds(10),
          Milliseconds(20),
          Milliseconds(96),
          Milliseconds(100),
          Milliseconds(102),
          Milliseconds(100),
          Milliseconds(200),
          Milliseconds(100)};
    ASSERT_EQ(collector.getMax(), ex);
    collector.add(LatencyCollector::LatencyInfo{.sourceTime = Milliseconds(100),
                                                .readTime = Milliseconds(101),
                                                .writeTime = Milliseconds(197),
                                                .commitTime = Milliseconds(298)});
    ASSERT_EQ(collector.getMax(), ex);
}

};  // namespace streams
