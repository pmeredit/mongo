#pragma once

#include <boost/optional.hpp>
#include <tuple>

#include "mongo/util/timer.h"

namespace streams {

// Reads a integer value from a file.
// If there is eny error it returns boost::none, otherwise it returns the value wrapped in
// boost::optional.
boost::optional<int64_t> readInt64ValueFromFile(const std::string& fileName);

// Reads usage_usec value from a cgroupv2 stats file.
// If there is eny error it returns boost::none, otherwise it returns the value wrapped in
// boost::optional.
boost::optional<int64_t> readCgroupV2StatFile(const std::string& fileName);

// Reads 2 integer values representing quota and period from cgroupv2 file.
// If it is not able to read the file according to this format, returns boost::none.
boost::optional<std::tuple<int64_t, int64_t>> readCgroupV2MaxFile(const std::string& fileName);


/*
** class Providing cGroup container utility methods
** abstracts cgroup v1 and cgroup v2
*/
class ContainerGroupStatsProvider {
public:
    ContainerGroupStatsProvider();
    virtual ~ContainerGroupStatsProvider(){};
    // get quota, period from cgroup files.
    // returns boost::none if we cannot read these values.
    virtual boost::optional<std::tuple<int64_t, int64_t>> getQuotaPeriod();
    // get cpuusage time in microseconds.
    virtual boost::optional<int64_t> getCpuTimeInMicroseconds();
    // get memory limit in microseconds
    virtual boost::optional<int64_t> getMemoryLimitBytes();
    // get memory usage bytes in microseconds
    virtual boost::optional<int64_t> getMemoryUsageBytes();

    // Utility methods to manipulate timer.
    virtual int64_t elapsedMicros() {
        return timer.micros();
    };
    virtual void resetTimer() {
        timer.reset();
    };

private:
    bool _isCgroupV2Container;
    mongo::Timer timer;
};

}  // namespace streams
