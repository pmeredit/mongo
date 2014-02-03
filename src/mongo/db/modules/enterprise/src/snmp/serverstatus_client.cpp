/*
 * Copyright (C) 2013 MongoDB, Inc.  All Rights Reserved.
 */

#include "serverstatus_client.h"

#include <algorithm>
#include <limits>


#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/db/client.h"
#include "mongo/platform/cstdint.h"
#include "mongo/util/log.h"
#include "mongo/util/timer.h"
#include "mongo/util/time_support.h"

namespace mongo {

const std::string ServerStatusClient::NO_EXTRA = "Uses default return values";
const std::string ServerStatusClient::ASSERTS = "asserts";
const std::string ServerStatusClient::BACKGROUND_FLUSHING = "backgroundFlushing";
const std::string ServerStatusClient::CONNECTIONS = "connections";
const std::string ServerStatusClient::CURSORS = "cursors";
const std::string ServerStatusClient::DUR = "dur";
const std::string ServerStatusClient::EXTRA_INFO = "extra_info";
const std::string ServerStatusClient::GLOBAL_LOCK = "globalLock";
const std::string ServerStatusClient::INDEX_COUNTERS = "indexCounters";
const std::string ServerStatusClient::LOCKS = "locks";
const std::string ServerStatusClient::NETWORK = "network";
const std::string ServerStatusClient::OPCOUNTERS = "opcounters";
const std::string ServerStatusClient::OPCOUNTERS_REPL = "opcountersRepl";
const std::string ServerStatusClient::RECORD_STATS = "recordStats";
const std::string ServerStatusClient::REPL = "repl";
const std::string ServerStatusClient::MEM = "mem";
const std::string ServerStatusClient::METRICS = "metrics";

const int ServerStatusClient::DATE_AND_TIME_TZ_LEN = 11;

ServerStatusClient::ServerStatusClient(const std::string& sectionName, time_t cacheExpireSecs)
        : _cacheExpireSecs(cacheExpireSecs), _lastRefresh(0), _sectionName(sectionName)
{
    std::vector<std::string> sectionList;
    sectionList.push_back(ASSERTS);
    sectionList.push_back(BACKGROUND_FLUSHING);
    sectionList.push_back(CONNECTIONS);
    sectionList.push_back(CURSORS);
    sectionList.push_back(DUR);
    sectionList.push_back(EXTRA_INFO);
    sectionList.push_back(GLOBAL_LOCK);
    sectionList.push_back(INDEX_COUNTERS);
    sectionList.push_back(LOCKS);
    sectionList.push_back(NETWORK);
    sectionList.push_back(OPCOUNTERS);
    sectionList.push_back(OPCOUNTERS_REPL);
    sectionList.push_back(RECORD_STATS);
    sectionList.push_back(REPL);
    sectionList.push_back(MEM);
    sectionList.push_back(METRICS);
    
    BSONObjBuilder b;
    b.append("serverStatus", 1);
    
    for (unsigned int jj=0; jj < sectionList.size(); ++jj) {
        if (sectionList[jj] == sectionName)
            b.append(sectionList[jj], 1);
        else
            b.append(sectionList[jj], 0);
    }
    
    _serverStatusCmd = b.obj();
}
        
bool ServerStatusClient::loadIfNeeded()
{    
    bool cachingEnabled = _cacheExpireSecs > 0;
    
    if (cachingEnabled)
    {
        time_t now = time(0);
        time_t secsFromLastRefresh = now - _lastRefresh;
         
        if (secsFromLastRefresh < _cacheExpireSecs)
        {
           return true;   
        } 
    }
     
    bool ok = load();
    
    if (ok && cachingEnabled)
    {
        _lastRefresh = time(0); // represents serverStatus completion
    }

    return ok;
}

bool ServerStatusClient::load()
{
    BSONObj response;
    bool ok = false;
    
    {
        Timer timer;
        ok = _dbClient.runCommand("admin", _serverStatusCmd, response);
        LOG(5) << "serverStatus cmd for " << _sectionName << " took " << timer.micros() 
               << " micros";
    }

    if (ok) {
        LOG(5) << "ServerStatusClient::load " << response ;
        _serverStatusData = response;
    }
    else {
        warning() << "serverStatus call failed: " << response.toString() << endl;
        _serverStatusData = BSONObj();
    }

    return ok;
}

BSONElement ServerStatusClient::getElement(const StringData& name)
{
    LOG(5) << "ServerStatusClient::getElement: " << name;
    
    // no need to handle return - fields will not be found in empty BSONObj 
    loadIfNeeded();
    
    return _serverStatusData.getFieldDotted(name);
}

bool ServerStatusClient::getBoolField(const StringData& name)
{
    return getElement(name).Bool();
}

int ServerStatusClient::getIntField(const StringData& name)
{
    return getElement(name).Int();
}

unsigned int ServerStatusClient::getDurationField(const StringData& name)
{
    int64_t timeMs = getInt64Field(name);

    // SNMP duration is 100ths of a second, represented as an unsigned int
    // In mongod we store as signed 64bit int in milliseconds so conversion is needed
    static const int64_t UINT32_MOD = static_cast<int64_t>(numeric_limits<unsigned int>::max())+1;
    int64_t timeSnmp = (timeMs/10) % UINT32_MOD;
    return static_cast<unsigned int>(timeSnmp);
}

int64_t ServerStatusClient::getInt64Field(const StringData& name)
{
    BSONElement elem = getElement(name);

    // We need to handle several potential return types for 64bit ints as mongod will
    // change type (for some metrics) depending on value to make more readable in the shell
    // (see https://github.com/mongodb/mongo/blob/r2.5.3/src/mongo/bson/bsonobjbuilder.h#L231)
    if (elem.type() == mongo::NumberInt) {
        return static_cast<int64_t>(elem.Int());
    }
    else if (elem.type() == mongo::NumberLong) {
        return elem.Long();
    }
    else {
        return static_cast<int64_t>(elem.Double());
    }
}

#if defined(_WIN32)
#pragma push_macro("snprintf")
#define snprintf _snprintf
#endif


// MIB does not provide 64bit integer type (outside of Counter64). 
// We represent with DisplayString (SIZE (0.. 22))
void ServerStatusClient::getInt64FieldAsString(const StringData& name, char* o_value, 
                                               int o_valueLen)
{
    verify(o_value);
    verify(o_valueLen > 0);
    
    int64_t int64Val = getInt64Field(name);
    
    std::ostringstream ostr;
    ostr << int64Val;
    
    int len = snprintf(o_value, o_valueLen, "%s", ostr.str().c_str());
    verify(len >= 0);
    verify(len < o_valueLen);
}

// MIB does not provide a floating point type. We represent with DisplayString (SIZE (0.. 16))
void ServerStatusClient::getDoubleField(const StringData& name, char* o_value, int o_valueLen)
{
    verify(o_value);
    verify(o_valueLen > 0);
    
    double dblVal = getElement(name).Double();
    
    int len = snprintf(o_value, o_valueLen, "%f", dblVal);
    verify(len >= 0);
    verify(len < o_valueLen);
}

#if defined(_WIN32)
#undef snprintf
#pragma pop_macro("snprintf")
#endif

void ServerStatusClient::getStringField(const StringData& name, char* o_value, int o_valueLen)
{
    verify(o_value);
    verify(o_valueLen > 0);
    
    BSONElement elem = getElement(name);
    if (elem.type() != mongo::String)
    {
        warning() << name << " is not a string" << endl;
        o_value[0] = '\0';
        return;
    }
    
    int size = elem.valuestrsize();
    memcpy(o_value, elem.valuestr(), std::min(size, o_valueLen));
    
    // if the value was larger than our buffer
    if (size > o_valueLen)
    {
        warning() << name << " value size " << size << " is larger than buffer size "
                  << o_valueLen << endl;
        o_value[o_valueLen-1] = '\0';
    }
}

void ServerStatusClient::getDateField(const StringData& name, char* o_value, int o_valueLen)
{
    verify(o_value);
    verify(o_valueLen >= DATE_AND_TIME_TZ_LEN);
    
    time_t date_timeT = getElement(name).Date().toTimeT();

    struct tm dateTime_tm = {0};
    unsigned short year = 0;

    time_t_to_Struct(date_timeT, &dateTime_tm);

    // The net-snmp DateAndTime conversion routines are internal only (in library/snmp-tc.h)
    // which is why we are implementing time_t to DateAndTime ourselves.
    year = dateTime_tm.tm_year + 1900;
    o_value[0] = static_cast<u_char>(year >> 8);
    o_value[1] = static_cast<u_char>(year);
    o_value[2] = dateTime_tm.tm_mon + 1;
    o_value[3] = dateTime_tm.tm_mday;
    o_value[4] = dateTime_tm.tm_hour;
    o_value[5] = dateTime_tm.tm_min;
    o_value[6] = dateTime_tm.tm_sec;
    o_value[7] = 0;
    o_value[8] = '+';
    o_value[9] = 0;
    o_value[10] = 0;
}

} // namespace mongo
