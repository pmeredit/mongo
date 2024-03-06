/*
 * Copyright (C) 2013 MongoDB, Inc.  All Rights Reserved.
 */

#include <cstdint>
#include <string>

#include "mongo/db/instance.h"

namespace mongo {


/**
 * An internal db client used to retrieve serverStatus metrics
 *
 * Currently date, int64_t and double get functions return strings formatted for SNMP -
 * this could be made more generic if we want to use outside of the SNMP module
 */
class ServerStatusClient {
    MONGO_DISALLOW_COPYING(ServerStatusClient);

public:
    ServerStatusClient(const std::string& sectionName, time_t cacheExpireSecs);

    bool getBoolField(StringData name);
    int getIntField(StringData name);
    unsigned int getDurationField(StringData name);
    int64_t getInt64Field(StringData name);
    void getInt64FieldAsString(StringData name, char* o_value, int o_valueLen);
    void getDoubleField(StringData name, char* o_value, int o_valueLen);
    void getStringField(StringData name, char* o_value, int o_valueLen);
    void getDateField(StringData name, char* o_value, int o_valueLen);

    static const std::string NO_EXTRA;
    static const std::string ASSERTS;
    static const std::string BACKGROUND_FLUSHING;
    static const std::string CONNECTIONS;
    static const std::string CURSORS;
    static const std::string DUR;
    static const std::string EXTRA_INFO;
    static const std::string GLOBAL_LOCK;
    static const std::string INDEX_COUNTERS;
    static const std::string LOCKS;
    static const std::string NETWORK;
    static const std::string OPCOUNTERS;
    static const std::string OPCOUNTERS_REPL;
    static const std::string RECORD_STATS;
    static const std::string REPL;
    static const std::string MEM;
    static const std::string METRICS;

    static const int DATE_AND_TIME_TZ_LEN;

private:
    BSONObj _serverStatusData;
    time_t _cacheExpireSecs;
    time_t _lastRefresh;
    BSONObj _serverStatusCmd;
    std::string _sectionName;

    bool load();
    bool loadIfNeeded();
    BSONElement getElement(StringData name);
};
}
