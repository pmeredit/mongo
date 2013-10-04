/*
 * Copyright (C) 2013 MongoDB, Inc.  All Rights Reserved.
 */

#include <string>

#include "mongo/db/instance.h"
#include "mongo/platform/cstdint.h"

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

        bool getBoolField(const StringData& name);
        int getIntField(const StringData& name);
        int64_t getInt64Field(const StringData& name);
        void getInt64FieldAsString(const StringData& name, char* o_value, int o_valueLen);
        void getDoubleField(const StringData& name, char* o_value, int o_valueLen);
        void getStringField(const StringData& name, char* o_value, int o_valueLen);
        void getDateField(const StringData& name, char* o_value, int o_valueLen);
        
        const static std::string ASSERTS;
        const static std::string BACKGROUND_FLUSHING;
        const static std::string CONNECTIONS;
        const static std::string CURSORS;
        const static std::string DUR;
        const static std::string EXTRA_INFO;
        const static std::string GLOBAL_LOCK;
        const static std::string INDEX_COUNTERS;
        const static std::string LOCKS;
        const static std::string NETWORK;
        const static std::string OPCOUNTERS;
        const static std::string OPCOUNTERS_REPL;
        const static std::string RECORD_STATS;
        const static std::string REPL;
        const static std::string MEM;
        const static std::string METRICS;
        
    private:
        DBDirectClient _dbClient;
        BSONObj        _serverStatusData;
        time_t         _cacheExpireSecs;
        time_t         _lastRefresh;
        BSONObj        _serverStatusCmd;
        std::string    _sectionName;

        bool load();
        bool loadIfNeeded();
        BSONElement getElement(const StringData& name);
    };

}
