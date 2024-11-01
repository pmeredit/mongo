/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <cstdint>
#include <filesystem>

namespace streams {

inline constexpr size_t operator""_KiB(unsigned long long v) {
    return v * 1024;
}

inline constexpr size_t operator""_MiB(unsigned long long v) {
    return v * 1024 * 1024;
}

inline constexpr size_t operator""_GiB(unsigned long long v) {
    return v * 1024 * 1024 * 1024;
}

}  // namespace streams
