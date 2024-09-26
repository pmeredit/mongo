/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include <fstream>
#include <limits>
#include <sys/vfs.h>
#include <vector>

#include "mongo/util/assert_util.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/str.h"
#include "streams/management/container_group_stats_provider.h"

namespace streams {

boost::optional<int64_t> readInt64ValueFromFile(const std::string& fileName) {
    std::string strValue;
    int64_t value;
    std::ifstream file(fileName.c_str());
    mongo::ScopeGuard guard([&] { file.close(); });
    if (!file.is_open()) {
        return boost::none;
    }
    file >> strValue;
    if (strValue.size() == 0) {
        return boost::none;
    }
    try {
        value = stoll(strValue);
    } catch (const std::exception&) {
        return boost::none;
    }
    // Some linux flavors will set the value to the maximum possible value.
    if (value == std::numeric_limits<int64_t>::max()) {
        return boost::none;
    }
    return {value};
}

boost::optional<int64_t> readCgroupV2StatFile(const std::string& fileName) {
    int64_t value;
    std::ifstream file(fileName);
    if (!file.is_open()) {
        return boost::none;
    }
    mongo::ScopeGuard guard([&] { file.close(); });
    std::string line;
    bool found = false;
    while (getline(file, line)) {
        std::vector<std::string> result;
        mongo::str::splitStringDelim(line, &result, ' ');
        if (result[0] == "usage_usec") {
            if (result.size() != 2) {
                return boost::none;
            }
            try {
                value = stoll(result[1]);
            } catch (const std::exception&) {
                return boost::none;
            }
            found = true;
            break;
        }
    }
    if (!found) {
        return boost::none;
    }
    return {value};
}

boost::optional<std::tuple<int64_t, int64_t>> readCgroupV2MaxFile(const std::string& fileName) {
    int64_t quota;
    int64_t period;
    std::ifstream file(fileName);
    mongo::ScopeGuard guard([&] { file.close(); });
    if (!file.is_open()) {
        return boost::none;
    }
    std::string line;

    if (!getline(file, line)) {
        return boost::none;
    }
    std::vector<std::string> result;
    mongo::str::splitStringDelim(line, &result, ' ');
    if (result.size() != 2) {
        return boost::none;
    }
    if (result[0] == "max") {
        return boost::none;
    }

    try {
        quota = strtoll(result[0].c_str(), nullptr, 10);
        period = strtoll(result[1].c_str(), nullptr, 10);
    } catch (const std::exception&) {
        return boost::none;
    }
    return {{quota, period}};
}

const std::string kBaseCgroupPathString = "/sys/fs/cgroup";
const std::string kCgroupCpuUsage = kBaseCgroupPathString + "/cpu/cpuacct.usage";
const std::string kCgroupCpuQuota = kBaseCgroupPathString + "/cpu/cpu.cfs_quota_us";
const std::string kCgroupCpuPeriod = kBaseCgroupPathString + "/cpu/cpu.cfs_period_us";
const std::string kCgroupMemUsageInBytes = kBaseCgroupPathString + "/memory/memory.usage_in_bytes";
const std::string kCgroupMemLimitInBytes = kBaseCgroupPathString + "/memory/memory.limit_in_bytes";
const std::string kCgroupV2CpuMax = kBaseCgroupPathString + "/cpu.max";
const std::string kCgroupV2CpuStat = kBaseCgroupPathString + "/cpu.stat";
const std::string kCgroupV2MemUsage = kBaseCgroupPathString + "/memory.current";
const std::string kCgroupV2MemLimit = kBaseCgroupPathString + "/memory.max";

ContainerGroupStatsProvider::ContainerGroupStatsProvider() {
    struct statfs fs_info;
    if (statfs(kBaseCgroupPathString.c_str(), &fs_info) == 0) {
        // Depending on linux version of the container,
        // the system will contain either cgroup or cgroupv2 metrics
        if (fs_info.f_type == 0x01021994 /* TMPFS_MAGIC from /usr/include/linux/magic.h */) {
            _isCgroupV2Container = false;
        } else if (fs_info.f_type ==
                   0x63677270 /* CGROUP2_SUPER_MAGIC from /usr/include/linux/magic.h */) {
            _isCgroupV2Container = true;
        } else {
            uasserted(9481407, "Invalid CGroup File System type");
        }
    } else {
        uasserted(9481408, "statfs call failed for /sys/fs/cgroup");
    }
}


boost::optional<std::tuple<int64_t, int64_t>> ContainerGroupStatsProvider::getQuotaPeriod() {
    if (_isCgroupV2Container) {
        return readCgroupV2MaxFile(kCgroupV2CpuMax);
    } else {
        auto quota = readInt64ValueFromFile(kCgroupCpuQuota);
        auto period = readInt64ValueFromFile(kCgroupCpuPeriod);
        if (quota && period) {
            return {{*quota, *period}};
        } else {
            return boost::none;
        }
    }
}

boost::optional<int64_t> ContainerGroupStatsProvider::getCpuTimeInMicroseconds() {
    if (_isCgroupV2Container) {
        return readCgroupV2StatFile(kCgroupV2CpuStat);
    } else {
        // cgroup v1 files return in nsec, we need to convert them to micro seconds;
        auto valueInNsec = readInt64ValueFromFile(kCgroupCpuUsage);
        return valueInNsec ? boost::optional<int64_t>{*valueInNsec / 1000} : valueInNsec;
    }
}

boost::optional<int64_t> ContainerGroupStatsProvider::getMemoryLimitBytes() {
    return _isCgroupV2Container ? readInt64ValueFromFile(kCgroupV2MemLimit)
                                : readInt64ValueFromFile(kCgroupMemLimitInBytes);
}

boost::optional<int64_t> ContainerGroupStatsProvider::getMemoryUsageBytes() {
    return _isCgroupV2Container ? readInt64ValueFromFile(kCgroupV2MemUsage)
                                : readInt64ValueFromFile(kCgroupMemUsageInBytes);
}

}  // namespace streams
