/*
 *    Copyright (C) 2019 10gen Inc.
 */

#pragma once

namespace mongo {

struct SnmpGlobalParams {
    bool enabled{false};
    bool subagent{true};
};

extern SnmpGlobalParams snmpGlobalParams;
}
