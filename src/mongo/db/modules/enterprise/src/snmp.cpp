/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

#include "mongo/platform/basic.h"

#ifdef _WIN32
// net-snmp uses this particular macro for some reason
#define WIN32
#endif

#include <boost/shared_ptr.hpp>
#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>
#include <signal.h>
 
#include "mongo/db/module.h"
#include "mongo/db/stats/counters.h"
#include "mongo/base/status.h"
#include "mongo/db/client.h"
#include "mongo/util/background.h"
#include "mongo/util/options_parser/option_description.h"
#include "mongo/util/time_support.h"
#include "mongo/util/assert_util.h"
#include "mongo/db/repl/replication_server_status.h"

#include "serverstatus_client.h"
#include "snmp.h"

namespace mongo {

    namespace moe = mongo::optionenvironment;

    static const int EXPIRE_SEC_METRICS = 0;            // atomic counters
    static const int EXPIRE_SEC_BACKGROUNDFLUSHING = 0; // global vars - no lock
    static const int EXPIRE_SEC_CURSORS = 1;            // ccmutex
    static const int EXPIRE_SEC_DUR = 0;                // global vars - no lock
    static const int EXPIRE_SEC_EXTRAINFO = 1;          // fscanf of /proc/pid/stat
    static const int EXPIRE_SEC_INDEXCOUNTERS = 0;      // global vars - no lock
    static const int EXPIRE_SEC_NETWORK = 1;            // counter lock
    static const int EXPIRE_SEC_GLOBALLOCK = 1;         // QLock
    static const int EXPIRE_SEC_REPL = 1;               // ReadContext on local 
                                                        // + ScopedDbConnection

    int my_snmp_callback( netsnmp_mib_handler *handler, netsnmp_handler_registration *reginfo,
                          netsnmp_agent_request_info *reqinfo, netsnmp_request_info *requests);

    

    class SNMPCallBack {
    public:
        SNMPCallBack( const string& name , const string& oid ) : _name( name ) , _oid( oid ) { }
        SNMPCallBack( const string& name , const SOID& oid ) : _name( name ) , _oid( oid ) { }
        
        virtual ~SNMPCallBack() { }
        
        virtual int respond( netsnmp_variable_list* var ) = 0;

        int init() {
            netsnmp_handler_registration * upreg = 
                netsnmp_create_handler_registration( _name.c_str() , &my_snmp_callback ,
                                                     _oid.getoid() , _oid.len() , 
                                                     HANDLER_CAN_RONLY );
            return netsnmp_register_instance( upreg );
        }


        
        bool operator==( const netsnmp_variable_list *var ) const {
            return _oid == var;
        }
        
        static const int RESPOND_OK = 0;
        
        
    private:
        string _name;
        SOID _oid;
    };


    namespace callbacks {

        class AtomicWordCallback : public SNMPCallBack {
        public:
            AtomicWordCallback( const char* name, const string& oid, const AtomicUInt* word ) 
                : SNMPCallBack( name, oid ), _word( word ) {
            }

            int respond( netsnmp_variable_list* var ) {
                unsigned val = _word->get();
                return snmp_set_var_typed_value(var, ASN_COUNTER, (u_char *) &val, sizeof(val) );
            }


        private:
            const AtomicUInt* _word;
                
        };

        class NameCallback : public SNMPCallBack {
        public:
            NameCallback() : SNMPCallBack( "serverName" , "1,1" ) {
                sprintf( _buf , "%s:%d" , getHostNameCached().c_str(), cmdLine.port );
                _len = strlen( _buf );
            }
            
            int respond( netsnmp_variable_list* var ) {
                return snmp_set_var_typed_value(var, ASN_OCTET_STR, (u_char *)_buf, _len );
            }

            char _buf[256]; // DisplayString (SIZE (0..255))
            size_t _len;
        };

        class UptimeCallback : public SNMPCallBack {
        public:
            UptimeCallback() : SNMPCallBack( "sysUpTime" , "1,2,2" ) {
                _startTime = curTimeMicros64();
            }
            
            int respond( netsnmp_variable_list* var ) {
                int uptime = ( curTimeMicros64() - _startTime ) / 10000;
                return snmp_set_var_typed_value(var, ASN_TIMETICKS, (u_char *) &uptime, 
                                                sizeof(uptime) );            
            }
            
            unsigned long long _startTime;
        };
        
        class MemoryCallback : public SNMPCallBack {
                        
            enum Type { RES , VIR , MAP } _type;

        public:
            static void addAll( vector<SNMPCallBack*>& v ) {
                v.push_back( new MemoryCallback( "memoryResident" , "1" , RES ) );
                v.push_back( new MemoryCallback( "memoryVirtual" , "2" , VIR ) );
                v.push_back( new MemoryCallback( "memoryMapped" , "3" , MAP ) );
            }

            int respond( netsnmp_variable_list* var ) {
                
                int val = 0;
                
                switch ( _type ) {
                case RES: {
                    ProcessInfo pi;
                    val = pi.getResidentSize();
                    break;
                }
                case VIR: {
                    ProcessInfo pi;
                    val = pi.getVirtualMemorySize();
                    break;
                }
                case MAP :
                    val = MemoryMappedFile::totalMappedLength() / ( 1024 * 1024 ) ;
                    break;
                }

                return snmp_set_var_typed_value(var, ASN_INTEGER, (u_char *) &val, sizeof(val) );
            }


        private:
            MemoryCallback( const string& name , const string& memsuffix , Type t ) 
                : SNMPCallBack( name , "1,4," + memsuffix ) , _type(t) {
            }
        };

        class ConnectionsCallback : public SNMPCallBack {
                
            enum Type { CURRENT, AVAILABLE, TOTAL } _type;

        public:
            static void addAll( vector<SNMPCallBack*>& v ) {
                v.push_back( new ConnectionsCallback ("connectionsCurrent" , "1" , CURRENT));
                v.push_back( new ConnectionsCallback ("connectionsAvailable" , "2" , AVAILABLE));
                v.push_back( new ConnectionsCallback ("connectionsTotalCreated" , "3" , TOTAL));
            }

            int respond( netsnmp_variable_list* var ) {
                
                int val = 0;
                u_char type = ASN_INTEGER;

                switch ( _type ) {
                case CURRENT: {
                    val = Listener::globalTicketHolder.used();
                    break;
                }
                case AVAILABLE: {
                    val = Listener::globalTicketHolder.available();
                    break;
                }
                case TOTAL:
                    type = ASN_COUNTER;
                    val = Listener::globalConnectionNumber.load();
                    break;
                }

                return snmp_set_var_typed_value( var, type, reinterpret_cast<u_char *>(&val),
                                                 sizeof(val) );

            }

        private:
            ConnectionsCallback( const std::string& name , const std::string& consuffix , Type t )
                : SNMPCallBack( name , "1,5," + consuffix ) , _type(t) {
            }
        };

        class AssertsCallback : public SNMPCallBack {

            enum Type { REGULAR, WARNING, MESSAGE, USER, ROLLOVER } _type;

        public:
            static void addAll( vector<SNMPCallBack*>& v ) {
                v.push_back( new AssertsCallback( "assertRegular" , "1" , REGULAR ) );
                v.push_back( new AssertsCallback( "assertWarning" , "2" , WARNING ) );
                v.push_back( new AssertsCallback( "assertMsg" , "3" , MESSAGE ) );
                v.push_back( new AssertsCallback( "assertUser" , "4" , USER ) );
                v.push_back( new AssertsCallback( "assertRollovers" , "5" , ROLLOVER ) );
            }

            int respond( netsnmp_variable_list* var ) {
                int val = 0;

                switch ( _type ) {
                case REGULAR:
                    val = assertionCount.regular;
                    break;
                case WARNING:
                    val = assertionCount.warning;
                    break;
                case MESSAGE:
                    val = assertionCount.msg;
                    break;
                case USER:
                    val = assertionCount.user;
                    break;
                case ROLLOVER:
                     val = assertionCount.rollovers;
                     break;
                default:
                    return -1;
                }

                return snmp_set_var_typed_value( var, ASN_COUNTER, 
                                                 reinterpret_cast<u_char *>(&val),
                                                 sizeof(val) );
            }

        private:
            AssertsCallback( const std::string& name, const std::string& suffix, Type t )
                : SNMPCallBack( name , "1,6," + suffix ) , _type(t) {
            }

        };
        
        class ServerStatusCallback : public SNMPCallBack {
               
        public:
            static void addAll( vector<SNMPCallBack*>& v ) {
                v.push_back(new ServerStatusCallback("flushCount", "1,7,1", 
                            ServerStatusClient::BACKGROUND_FLUSHING, 
                            "backgroundFlushing.flushes", VT_CNT32));
                v.push_back(new ServerStatusCallback("flushTotalMs", "1,7,2",
                            ServerStatusClient::BACKGROUND_FLUSHING, 
                            "backgroundFlushing.total_ms", VT_INT32));
                v.push_back(new ServerStatusCallback("flushAverageMs", "1,7,3",
                            ServerStatusClient::BACKGROUND_FLUSHING,
                            "backgroundFlushing.average_ms", VT_DOUBLE));
                v.push_back(new ServerStatusCallback("flushLastMs", "1,7,4",
                            ServerStatusClient::BACKGROUND_FLUSHING,
                            "backgroundFlushing.last_ms", VT_INT32));
                v.push_back(new ServerStatusCallback("flushLastDateTime", "1,7,5",
                            ServerStatusClient::BACKGROUND_FLUSHING,
                            "backgroundFlushing.last_finished", VT_DATE));
                v.push_back(new ServerStatusCallback("cursorTotalOpen", "1,8,1",
                            ServerStatusClient::CURSORS, "cursors.totalOpen", VT_INT32));
                v.push_back(new ServerStatusCallback("cursorClientSize", "1,8,2", 
                            ServerStatusClient::CURSORS, "cursors.clientCursors_size", VT_INT32));
                v.push_back(new ServerStatusCallback("cursorTimedOut", "1,8,3",
                            ServerStatusClient::CURSORS, "cursors.timedOut", VT_INT32));
                v.push_back(new ServerStatusCallback("durCommits", "1,9,1",
                            ServerStatusClient::DUR, "dur.commits", VT_CNT32));
                v.push_back(new ServerStatusCallback("durJournaledMb", "1,9,2",
                            ServerStatusClient::DUR, "dur.journaledMB", VT_DOUBLE));
                v.push_back(new ServerStatusCallback("durWritesToDataFilesMb", "1,9,3",
                            ServerStatusClient::DUR, "dur.writeToDataFilesMB", VT_DOUBLE));
                v.push_back(new ServerStatusCallback("durCompression", "1,9,4",
                            ServerStatusClient::DUR, "dur.compression", VT_DOUBLE));
                v.push_back(new ServerStatusCallback("durCommitsInWriteLock", "1,9,5",
                            ServerStatusClient::DUR, "dur.commitsInWriteLock", VT_INT32));
                v.push_back(new ServerStatusCallback("durEarlyCommits", "1,9,6", 
                            ServerStatusClient::DUR, "dur.earlyCommits", VT_INT32));
                v.push_back(new ServerStatusCallback("durTimeMsDt", "1,9,7,1", 
                            ServerStatusClient::DUR, "dur.timeMs.dt", VT_INT32));
                v.push_back(new ServerStatusCallback("durTimeMsPrepLogBuffer", "1,9,7,2",
                            ServerStatusClient::DUR, "dur.timeMs.prepLogBuffer", VT_INT32));
                v.push_back(new ServerStatusCallback("durTimeMsWriteToJournal", "1,9,7,3",
                            ServerStatusClient::DUR, "dur.timeMs.writeToJournal", VT_INT32));
                v.push_back(new ServerStatusCallback("durTimeMsWriteToDataFiles", "1,9,7,4",
                            ServerStatusClient::DUR, "dur.timeMs.writeToDataFiles", VT_INT32));
                v.push_back(new ServerStatusCallback("durTimeMsRemapPrivateView", "1,9,7,5",
                            ServerStatusClient::DUR, "dur.timeMs.remapPrivateView", VT_INT32));
                v.push_back(new ServerStatusCallback("extraInfoNote", "1,10,1",
                            ServerStatusClient::EXTRA_INFO, "extra_info.note", VT_STRING));
                {
                    ServerStatusCallback* sscb = 
                        new ServerStatusCallback("extraInfoHeapUsageBytes", "1,10,2",
                                                ServerStatusClient::EXTRA_INFO,
                                                "extra_info.heap_usage_bytes", VT_INT32);
                    sscb->_linuxOnly = true;
                    v.push_back(sscb);
                }
                v.push_back(new ServerStatusCallback("extraInfoPageFaults", "1,10,3",
                            ServerStatusClient::EXTRA_INFO, "extra_info.page_faults", VT_CNT32));
                v.push_back(new ServerStatusCallback("indexCounterAccesses", "1,11,1",
                            ServerStatusClient::INDEX_COUNTERS, "indexCounters.accesses",
                            VT_INT32));
                v.push_back(new ServerStatusCallback("indexCounterHits", "1,11,2",
                            ServerStatusClient::INDEX_COUNTERS, "indexCounters.hits", VT_INT32));
                v.push_back(new ServerStatusCallback("indexCounterMisses", "1,11,3",
                            ServerStatusClient::INDEX_COUNTERS, "indexCounters.misses", VT_INT32));
                v.push_back(new ServerStatusCallback("indexCounterResets", "1,11,4",
                            ServerStatusClient::INDEX_COUNTERS, "indexCounters.resets", VT_INT32));
                v.push_back(new ServerStatusCallback("indexCounterMissRatio", "1,11,5",
                            ServerStatusClient::INDEX_COUNTERS, "indexCounters.missRatio",
                            VT_DOUBLE));
                v.push_back(new ServerStatusCallback("networkBytesIn", "1,12,1",
                            ServerStatusClient::NETWORK, "network.bytesIn", VT_INT32));
                v.push_back(new ServerStatusCallback("networkBytesOut", "1,12,2",
                            ServerStatusClient::NETWORK, "network.bytesOut", VT_INT32));
                v.push_back(new ServerStatusCallback("networkNumRequests", "1,12,3",
                            ServerStatusClient::NETWORK, "network.numRequests", VT_CNT32));
                v.push_back(new ServerStatusCallback("writeBacksQueued", "1,13",
                            ServerStatusClient::METRICS, "writeBacksQueued", VT_BOOL));
                v.push_back(new ServerStatusCallback("globalLockTotalTime", "1,14,1", 
                            ServerStatusClient::GLOBAL_LOCK, "globalLock.totalTime", VT_INT64));
                v.push_back(new ServerStatusCallback("globalLockLockTime", "1,14,2",
                            ServerStatusClient::GLOBAL_LOCK, "globalLock.lockTime", VT_INT64));
                v.push_back(new ServerStatusCallback("globalLockCurrentQueue", "1,14,3,1",
                            ServerStatusClient::GLOBAL_LOCK, "globalLock.currentQueue.total",
                            VT_INT32));
                v.push_back(new ServerStatusCallback("globalLockCurrentQueueReaders", "1,14,3,2",
                            ServerStatusClient::GLOBAL_LOCK, "globalLock.currentQueue.readers",
                            VT_INT32));
                v.push_back(new ServerStatusCallback("globalLockCurrentQueueWriters", "1,14,3,3",
                            ServerStatusClient::GLOBAL_LOCK, "globalLock.currentQueue.writers",
                            VT_INT32));
                v.push_back(new ServerStatusCallback("globalLockActiveClientsTotal", "1,14,4,1",
                            ServerStatusClient::GLOBAL_LOCK, "globalLock.activeClients.total",
                            VT_INT32));
                v.push_back(new ServerStatusCallback("globalLockActiveClientsReaders", "1,14,4,2",
                            ServerStatusClient::GLOBAL_LOCK, "globalLock.activeClients.readers",
                            VT_INT32));
                v.push_back(new ServerStatusCallback("globalLockActiveClientsWriters", "1,14,4,3",
                            ServerStatusClient::GLOBAL_LOCK, "globalLock.activeClients.writers",
                            VT_INT32));
                v.push_back(new ServerStatusCallback("metricsDocumentDeleted", "1,15,1,1",
                            ServerStatusClient::METRICS, "metrics.document.deleted", VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsDocumentInserted", "1,15,1,2",
                            ServerStatusClient::METRICS, "metrics.document.inserted", VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsDocumentReturned", "1,15,1,3",
                            ServerStatusClient::METRICS, "metrics.document.returned", VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsDocumentUpdated", "1,15,1,4",
                            ServerStatusClient::METRICS, "metrics.document.updated", VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsGetLastErrorWtimeNum", "1,15,2,1,1",
                            ServerStatusClient::METRICS, "metrics.getLastError.wtime.num",
                            VT_INT32));
                v.push_back(new ServerStatusCallback("metricsGetLastErrorWtimeTotalMillis",
                            "1,15,2,1,2", ServerStatusClient::METRICS, 
                            "metrics.getLastError.wtime.totalMillis", VT_INT32));
                v.push_back(new ServerStatusCallback("meticsGetLastErrorWtimeouts", "1,15,2,2",
                            ServerStatusClient::METRICS, "metrics.getLastError.wtimeouts",
                            VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsOperationFastmod", "1,15,3,1",
                            ServerStatusClient::METRICS, "metrics.operation.fastmod", VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsOperationIdhack", "1,15,3,2",
                            ServerStatusClient::METRICS, "metrics.operation.idhack", VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsOperationScanAndOrder", "1,15,3,3",
                            ServerStatusClient::METRICS, "metrics.operation.scanAndOrder",
                            VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsQueryExecutorScanned", "1,15,4,1",
                            ServerStatusClient::METRICS, "metrics.queryExecutor.scanned",
                            VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsRecordMoves", "1,15,5,1",
                            ServerStatusClient::METRICS, "metrics.record.moves", VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsReplApplyBatchesNum", "1,15,6,1,1,1",
                            ServerStatusClient::METRICS, "metrics.repl.apply.batches.num",
                            VT_INT32));
                v.push_back(new ServerStatusCallback("metricsReplApplyBatchesTotalMillis",
                            "1,15,6,1,1,2", ServerStatusClient::METRICS,
                            "metrics.repl.apply.batches.totalMillis", VT_INT32));
                v.push_back(new ServerStatusCallback("metricsReplApplyOps", "1,15,6,1,2",
                            ServerStatusClient::METRICS, "metrics.repl.apply.ops", VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsReplBufferCount", "1,15,6,2,1",
                            ServerStatusClient::METRICS, "metrics.repl.buffer.count", VT_INT64));
                v.push_back(new ServerStatusCallback("metricsReplBufferMaxSizeBytes", "1,15,6,2,2",
                            ServerStatusClient::METRICS, "metrics.repl.buffer.maxSizeBytes",
                            VT_INT32));
                v.push_back(new ServerStatusCallback("metricsReplBufferSizeBytes", "1,15,6,2,3",
                            ServerStatusClient::METRICS, "metrics.repl.buffer.sizeBytes",
                            VT_INT64));
                v.push_back(new ServerStatusCallback("metricsReplNetworkBytes", "1,15,6,3,1",
                            ServerStatusClient::METRICS, "metrics.repl.network.bytes", VT_INT64));
                v.push_back(new ServerStatusCallback("metricsReplNetworkGetmoresNum",
                            "1,15,6,3,2,1", ServerStatusClient::METRICS, 
                            "metrics.repl.network.getmores.num", VT_INT32));
                v.push_back(new ServerStatusCallback("metricsReplNetworkGetmoresTotalMillis",
                            "1,15,6,3,2,2", ServerStatusClient::METRICS,
                            "metrics.repl.network.getmores.totalMillis", VT_INT32));
                v.push_back(new ServerStatusCallback("metricsReplNetworkOps", "1,15,6,3,3",
                            ServerStatusClient::METRICS, "metrics.repl.network.ops", VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsReplNetworkReadersCreated",
                            "1,15,6,3,4", ServerStatusClient::METRICS, 
                            "metrics.repl.network.readersCreated", VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsReplOplogInsertNum", "1,15,6,4,1,1",
                            ServerStatusClient::METRICS, "metrics.repl.oplog.insert.num",
                            VT_CNT32));
                v.push_back(new ServerStatusCallback("metricsReplOplogInsertTotalMillis",
                            "1,15,6,4,1,2", ServerStatusClient::METRICS,
                            "metrics.repl.oplog.insert.totalMillis", VT_INT32));
                v.push_back(new ServerStatusCallback("metricsReplOplogInsertBytes", "1,15,6,4,2",
                            ServerStatusClient::METRICS, "metrics.repl.oplog.insertBytes",
                            VT_INT64));
                v.push_back(new ServerStatusCallback("metricsReplPreloadDocsNum", "1,15,6,5,1,1",
                            ServerStatusClient::METRICS, "metrics.repl.preload.docs.num",
                            VT_CNT32));
                v.push_back(new ServerStatusCallback("metricsReplPreloadDocsTotalMillis",
                            "1,15,6,5,1,2", ServerStatusClient::METRICS,
                            "metrics.repl.preload.docs.totalMillis", VT_INT32));
                v.push_back(new ServerStatusCallback("metricsReplPreloadIndexesNum",
                            "1,15,6,5,2,1", ServerStatusClient::METRICS,
                            "metrics.repl.preload.indexes.num", VT_CNT32));
                v.push_back(new ServerStatusCallback("metricsReplPreloadIndexesTotalMillis",
                            "1,15,6,5,2,2", ServerStatusClient::METRICS,
                            "metrics.repl.preload.indexes.totalMillis", VT_INT32));
                v.push_back(new ServerStatusCallback("metricsTtlDeletedDocuments", "1,15,7,1",
                            ServerStatusClient::METRICS, "metrics.ttl.deletedDocuments",
                            VT_CNT64));
                v.push_back(new ServerStatusCallback("metricsTtlPasses", "1,15,7,2",
                            ServerStatusClient::METRICS, "metrics.ttl.passes", VT_CNT64));
                {
                    ServerStatusCallback* sscb = 
                        new ServerStatusCallback("replSetName", "1,16,1",
                                        ServerStatusClient::REPL, "repl.setName", VT_STRING);
                    sscb->_replicaSetOnly = true; 
                    v.push_back(sscb);
                    
                    sscb =  new ServerStatusCallback("replSetVersion", "1,16,2",
                                        ServerStatusClient::REPL, "repl.setVersion", VT_INT32);
                    sscb->_replicaSetOnly = true; 
                    v.push_back(sscb);
                    
                    sscb =  new ServerStatusCallback("replIsMaster", "1,16,3",
                                        ServerStatusClient::REPL, "repl.ismaster", VT_BOOL);
                    sscb->_replicaSetOnly = true; 
                    v.push_back(sscb);

                    sscb =  new ServerStatusCallback("replIsSecondary", "1,16,4",
                                        ServerStatusClient::REPL, "repl.secondary", VT_BOOL);
                    sscb->_replicaSetOnly = true; 
                    v.push_back(sscb);

                    sscb =  new ServerStatusCallback("replPrimary", "1,16,5",
                                        ServerStatusClient::REPL, "repl.primary", VT_STRING);
                    sscb->_replicaSetOnly = true; 
                    v.push_back(sscb);

                    sscb =  new ServerStatusCallback("replMe", "1,16,6",
                                        ServerStatusClient::REPL, "repl.me", VT_STRING);
                    sscb->_replicaSetOnly = true; 
                    v.push_back(sscb);
                }
            }

            int respond( netsnmp_variable_list* var ) {
                int val = 0;
                int64_t val64 = 0;
                char buf[256]; // DisplayString max is 255 chars
                buf[0] = '\0';
                
                
                if (isValidMetric()) {
                    ServerStatusClient& ssClient = getServerStatusClient(_serverStatusSection);
                    
                    switch (_metricType) {
                    case VT_INT32:
                    case VT_CNT32:
                        val = ssClient.getIntField(_serverStatusMetric);
                        break;
                    case VT_CNT64:
                        val64 = ssClient.getInt64Field(_serverStatusMetric);
                        break;
                    case VT_BOOL:
                        if (ssClient.getBoolField(_serverStatusMetric))
                            val = 1; 
                        break;
                    case VT_INT64:
                        ssClient.getInt64FieldAsString(_serverStatusMetric, buf, sizeof(buf));
                        break;
                    case VT_DOUBLE:
                        ssClient.getDoubleField(_serverStatusMetric, buf, sizeof(buf));
                        break;
                    case VT_STRING:
                        ssClient.getStringField(_serverStatusMetric, buf, sizeof(buf));
                        break;
                    case VT_DATE:
                        ssClient.getDateField(_serverStatusMetric, buf, sizeof(buf));
                        break;
                    default:
                        break;
                    }
                }
                
                switch (_snmpType) {
                case ASN_COUNTER64:
                {   
                    struct counter64 c64;
                    c64.low = val64;
                    c64.high = val64 >> 32;
                       
                    return snmp_set_var_typed_value(var, ASN_COUNTER64,
                                                    reinterpret_cast<u_char *>(&c64),
                                                    sizeof(val64));
                }
                case ASN_OCTET_STR:
                    return snmp_set_var_typed_value(var, ASN_OCTET_STR, 
                                                    reinterpret_cast<u_char *>(buf),
                                                    strlen(buf));
                case ASN_INTEGER:
                case ASN_COUNTER:
                default:
                    return snmp_set_var_typed_value(var, _snmpType, 
                                                    reinterpret_cast<u_char *>(&val),
                                                    sizeof(val));
                }
            }

        private:
            // ValueType represents the type of a metric and is a
            // bridge between the serverStatus type and SNMP type
            enum ValueType {VT_INT32, VT_CNT32, VT_BOOL, VT_INT64, VT_CNT64, VT_STRING, 
                            VT_DATE, VT_DOUBLE} _metricType;
            
            ServerStatusCallback(const std::string& name, const std::string& suffix,
                                 const std::string& section, const std::string& metric,
                                 ValueType metricType)
                : SNMPCallBack(name, suffix ),
                  _metricType(metricType), _serverStatusSection(section),
                  _serverStatusMetric(metric),_replicaSetOnly(false), _linuxOnly(false),
                  _snmpType(ASN_INTEGER) {
                    
                switch (_metricType) {
                case VT_CNT64:
                    _snmpType = ASN_COUNTER64;
                    break;
                case VT_CNT32:
                    _snmpType = ASN_COUNTER;
                    break;
                case VT_STRING:
                case VT_DATE:
                case VT_DOUBLE:
                case VT_INT64:
                    _snmpType = ASN_OCTET_STR;
                    break;
                case VT_INT32:
                case VT_BOOL:
                default:
                    _snmpType = ASN_INTEGER;
                    break;
                }
            }
            
            // returns whether the current metric is valid for this mongod instance
            bool isValidMetric() const {
             
                if (_replicaSetOnly && !anyReplEnabled()) {
                    return false;
                }
                    
               
#ifndef __linux__   // if OS is not linux and metric is linux-only
                if (_linuxOnly) {
                    return false;
                }              
#endif
                    
                return true;
            }
            
            
            static int getSectionTimeoutSecs(const std::string& section) {
                if (section == ServerStatusClient::METRICS)
                    return EXPIRE_SEC_METRICS;
                
                if (section == ServerStatusClient::BACKGROUND_FLUSHING)
                    return EXPIRE_SEC_BACKGROUNDFLUSHING;
                
                if (section == ServerStatusClient::CURSORS)
                    return EXPIRE_SEC_CURSORS;
                
                if (section == ServerStatusClient::DUR)
                    return EXPIRE_SEC_DUR;
                
                if (section == ServerStatusClient::EXTRA_INFO)
                    return EXPIRE_SEC_EXTRAINFO;
                
                if (section == ServerStatusClient::INDEX_COUNTERS)
                    return EXPIRE_SEC_INDEXCOUNTERS;
                
                if (section == ServerStatusClient::NETWORK)
                    return EXPIRE_SEC_NETWORK;
                
                if (section == ServerStatusClient::GLOBAL_LOCK)
                    return EXPIRE_SEC_GLOBALLOCK;
                
                if (section == ServerStatusClient::REPL)
                    return EXPIRE_SEC_REPL;
                    
                return 0;  
            }
            
            static ServerStatusClient& getServerStatusClient(const std::string& section) {
                
                std::map< std::string, boost::shared_ptr<ServerStatusClient> >::iterator it;
                it = _serverStatusClientMap.find(section);
                
                if (it == _serverStatusClientMap.end()) {
                    
                    _serverStatusClientMap[section].reset(
                            new ServerStatusClient(section, getSectionTimeoutSecs(section)));
                }
                
                return *_serverStatusClientMap[section];
            }
            
            std::string _serverStatusSection;
            std::string _serverStatusMetric;
            bool _replicaSetOnly;
            bool _linuxOnly;
            u_char _snmpType;
            
            static std::map< std::string,
                             boost::shared_ptr<ServerStatusClient> > _serverStatusClientMap;
        };
        
        std::map< std::string, boost::shared_ptr<ServerStatusClient> > 
                                ServerStatusCallback::_serverStatusClientMap;

    }

    
    class SNMPAgent : public BackgroundJob , Module {
    public:

        SNMPAgent()
            : Module( "snmp" ) {

            _enabled = 0;
            _subagent = 1;
            _snmpIterations = 0;
            _numThings = 0;
            _agentName = "mongod";

        }

        ~SNMPAgent() {
        }

        virtual string name() const { return "SNMPAgent"; }

        void addOptions(moe::OptionSection* options) {

            typedef moe::OptionDescription OD;

            moe::OptionSection snmp_options("SNMP Module Options");

            Status ret = snmp_options.addOption(OD("snmp-subagent", "snmp-subagent", moe::Switch,
                        "run snmp subagent", true));
            if (!ret.isOK()) {
                log() << "Failed to register snmp-subagent option: " << ret.toString() << endl;
                return;
            }
            ret = snmp_options.addOption(OD("snmp-master", "snmp-master", moe::Switch,
                        "run snmp as master", true));
            if (!ret.isOK()) {
                log() << "Failed to register snmp-master option: " << ret.toString() << endl;
                return;
            }
            ret = options->addSection(snmp_options);
            if (!ret.isOK()) {
                log() << "Failed to add snmp option section: " << ret.toString() << endl;
                return;
            }
        }

        void config(moe::Environment& params) {
            if ( params.count( "snmp-subagent" ) ) {
                enable();
            }
            if ( params.count( "snmp-master" ) ) {
                makeMaster();
                enable();
            }
        }

        void enable() {
            _enabled = 1;
        }

        void makeMaster() {
            _subagent = false;
        }

        void init() {
            oidManager.init();
            go();
        }

        void shutdown() {
            _enabled = 0;
        }

        void run() {
            
            while ( ! _enabled ) {
                LOG(1) << "SNMPAgent not enabled";
                return;
            }

            snmp_enable_stderrlog();

            if ( _subagent ) {
                if ( netsnmp_ds_set_boolean(NETSNMP_DS_APPLICATION_ID, NETSNMP_DS_AGENT_ROLE, 1)
                         != SNMPERR_SUCCESS ) {
                    log() << "SNMPAgent faild setting subagent" << endl;
                    return;
                }
            }

            SOCK_STARTUP;

            init_agent( _agentName.c_str() );

            _init();
            LOG(1) << "SNMPAgent num things: " << _numThings;

            init_snmp( _agentName.c_str() );

            if ( ! _subagent ) {
                int res = init_master_agent();
                if ( res ) {
                    warning() << "error starting SNMPAgent as master err:" << res << endl;
                    return;
                }
                log() << "SNMPAgent running as master" << endl;
            }
            else {
                log() << "SNMPAgent running as subagent" << endl;
            }

            while( _enabled && ! inShutdown() ) {
                _snmpIterations++;
                agent_check_and_process(1);
            }

            log() << "SNMPAgent shutting down" << endl;
            snmp_shutdown( _agentName.c_str() );
            SOCK_CLEANUP;
        }

        SNMPCallBack* getCallBack( const netsnmp_variable_list* var ) const {
            for ( unsigned i=0; i<_callbacks.size(); i++ )
                if ( *_callbacks[i] == var )
                    return _callbacks[i];
            return 0;
        }

    private:

        void _checkRegister( int x ) {
            if ( x == MIB_REGISTERED_OK ) {
                _numThings++;
                return;
            }

            if ( x == MIB_REGISTRATION_FAILED ) {
                log() << "SNMPAgent MIB_REGISTRATION_FAILED!" << endl;
            }
            else if ( x == MIB_DUPLICATE_REGISTRATION ) {
                log() << "SNMPAgent MIB_DUPLICATE_REGISTRATION!" << endl;
            }
            else {
                log() << "SNMPAgent unknown registration failure" << endl;
            }

        }

        void _initCounter( const char * name , const char* oidhelp , int * counter ) {
            LOG(2) << "registering: " << name << " " << oidhelp;

            netsnmp_handler_registration * reg = 
                netsnmp_create_handler_registration( name , NULL,
                                                     oidManager.getoid( oidhelp ), 
                                                     oidManager.len( oidhelp ) ,
                                                     HANDLER_CAN_RONLY);
            
            netsnmp_watcher_info * winfo = 
                netsnmp_create_watcher_info( counter, sizeof(int),
                                             ASN_COUNTER, WATCHER_FIXED_SIZE);
            
            _checkRegister( netsnmp_register_watched_scalar( reg, winfo ) );
        }

        void _initCounter( const char * name , const char* oidhelp , AtomicUInt * counter ) {
            LOG(2) << "registering: " << name << " " << oidhelp;

            netsnmp_handler_registration * reg = 
                netsnmp_create_handler_registration( name , NULL,
                                                     oidManager.getoid( oidhelp ),
                                                     oidManager.len( oidhelp ) ,
                                                     HANDLER_CAN_RONLY);

            unsigned * u = (unsigned*)counter;

            netsnmp_watcher_info * winfo = 
                netsnmp_create_watcher_info( u , sizeof(unsigned),
                                             ASN_COUNTER, WATCHER_FIXED_SIZE);
            
            _checkRegister( netsnmp_register_watched_scalar( reg, winfo ) );
        }


        void _init() {
            
            // add all callbacks
            _callbacks.push_back( new callbacks::UptimeCallback() );
            _callbacks.push_back( new callbacks::NameCallback() );
            callbacks::MemoryCallback::addAll( _callbacks );
            callbacks::ConnectionsCallback::addAll( _callbacks );
            callbacks::AssertsCallback::addAll( _callbacks );
            callbacks::ServerStatusCallback::addAll( _callbacks );
                
                
            // static counters
            
            //  ---- globalOpCounters
            _callbacks.push_back( new callbacks::AtomicWordCallback( "globalOpInsert" , "1,3,1,1" ,
                                  globalOpCounters.getInsert() ) );
            _callbacks.push_back( new callbacks::AtomicWordCallback( "globalOpInsert" , "1,3,1,2" ,
                                  globalOpCounters.getQuery() ) );
            _callbacks.push_back( new callbacks::AtomicWordCallback( "globalOpInsert" , "1,3,1,3" ,
                                  globalOpCounters.getUpdate() ) );
            _callbacks.push_back( new callbacks::AtomicWordCallback( "globalOpInsert" , "1,3,1,4" ,
                                  globalOpCounters.getDelete() ) );
            _callbacks.push_back( new callbacks::AtomicWordCallback( "globalOpInsert" , "1,3,1,5" ,
                                  globalOpCounters.getGetMore() ) );

            // register all callbacks
            for ( unsigned i=0; i<_callbacks.size(); i++ )
                _checkRegister( _callbacks[i]->init() );

            Client::initThread("SnmpAgent");
            
        }


        string _agentName;

        bool _enabled;
        bool _subagent;

        int _numThings;
        int _snmpIterations;

        vector<SNMPCallBack*> _callbacks;

    } snmpAgent;

    int my_snmp_callback( netsnmp_mib_handler *handler, netsnmp_handler_registration *reginfo,
                          netsnmp_agent_request_info *reqinfo, netsnmp_request_info *requests) {

        while (requests) {
            netsnmp_variable_list *var = requests->requestvb;
            
            switch (reqinfo->mode) {
            case MODE_GET: {
                SNMPCallBack * cb = snmpAgent.getCallBack( var );
                if ( !cb ) {
                    warning() << "no callback for: " << oidManager.toString( var->name ) << endl;
                }
                else
                {
                    try {
                        if ( cb->respond( var ) != SNMPCallBack::RESPOND_OK ) {
                            warning() << "error retrieving: " << oidManager.toString( var->name )
                                      << endl;
                        }
                    }
                    catch (std::exception& e) {
                        warning() << "exception on retrieval of " 
                                  << oidManager.toString( var->name )
                                  << ": " << e.what() << endl;
                    }
                }

                return SNMP_ERR_NOERROR;
            }

            case MODE_GETNEXT: {
                /*
                  not sure where this came from or if its remotely correct
                if ( snmpAgent._uptime == var ) {
                    snmp_set_var_objid(var,snmpAgent._uptime.getoid() , snmpAgent._uptime.len() );
                    snmp_set_var_typed_value(var, ASN_TIMETICKS, (u_char *) &uptime, 
                                             sizeof(uptime) );
                    return SNMP_ERR_NOERROR;
                }
                */
                warning() << "i have no idea what i'm supposed to do with MODE_GETNEXT "
                          << __FILE__ << ":" << __LINE__ << endl;
                break;
            }
            default:
                netsnmp_set_request_error(reqinfo, requests, SNMP_ERR_GENERR);
                break;
            }

            requests = requests->next;
        }
        return SNMP_ERR_NOERROR;
    }
}

