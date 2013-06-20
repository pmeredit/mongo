// snmp.cpp

#include "mongo/platform/basic.h"

#ifdef _WIN32
// net-snmp uses this particular macro for some reason
#define WIN32
#endif

#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>
#include <signal.h>
#include <boost/program_options.hpp>
 
#include "mongo/db/module.h"
#include "mongo/db/stats/counters.h"
#include "mongo/util/background.h"
#include "mongo/util/time_support.h"

#include "snmp.h"

namespace mongo {

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

            char _buf[512];
            size_t _len;
        };

        class UptimeCallback : public SNMPCallBack {
        public:
            UptimeCallback() : SNMPCallBack( "sysUpTime" , "1,2,2" ) {
                _startTime = curTimeMicros64();
            }
            
            int respond( netsnmp_variable_list* var ) {
                int uptime = ( curTimeMicros64() - _startTime ) / 10000;
                return snmp_set_var_typed_value(var, ASN_TIMETICKS, (u_char *) &uptime, sizeof(uptime) );            
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
                
                int val;
                
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
            
            add_options()
            ( "snmp-subagent" , "run snmp subagent" )
            ( "snmp-master" , "run snmp as master" )
            ;

        }

        ~SNMPAgent() {
        }

        virtual string name() const { return "SNMPAgent"; }

        void config( boost::program_options::variables_map& params ) {
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
                LOG(1) << "SNMPAgent not enabled" << endl;
                return;
            }

            snmp_enable_stderrlog();

            if ( _subagent ) {
                if ( netsnmp_ds_set_boolean(NETSNMP_DS_APPLICATION_ID, NETSNMP_DS_AGENT_ROLE, 1) != SNMPERR_SUCCESS ) {
                    log() << "SNMPAgent faild setting subagent" << endl;
                    return;
                }
            }

            SOCK_STARTUP;

            init_agent( _agentName.c_str() );

            _init();
            LOG(1) << "SNMPAgent num things: " << _numThings << endl;

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
            LOG(2) << "registering: " << name << " " << oidhelp << endl;

            netsnmp_handler_registration * reg = 
                netsnmp_create_handler_registration( name , NULL,
                                                     oidManager.getoid( oidhelp ) , oidManager.len( oidhelp ) ,
                                                     HANDLER_CAN_RONLY);
            
            netsnmp_watcher_info * winfo = 
                netsnmp_create_watcher_info( counter, sizeof(int),
                                             ASN_COUNTER, WATCHER_FIXED_SIZE);
            
            _checkRegister( netsnmp_register_watched_scalar( reg, winfo ) );
        }

        void _initCounter( const char * name , const char* oidhelp , AtomicUInt * counter ) {
            LOG(2) << "registering: " << name << " " << oidhelp << endl;

            netsnmp_handler_registration * reg = 
                netsnmp_create_handler_registration( name , NULL,
                                                     oidManager.getoid( oidhelp ) , oidManager.len( oidhelp ) ,
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

            // static counters
            
            //  ---- globalOpCounters
            _callbacks.push_back( new callbacks::AtomicWordCallback( "globalOpInsert" , "1,3,1,1" , globalOpCounters.getInsert() ) );
            _callbacks.push_back( new callbacks::AtomicWordCallback( "globalOpInsert" , "1,3,1,2" , globalOpCounters.getQuery() ) );
            _callbacks.push_back( new callbacks::AtomicWordCallback( "globalOpInsert" , "1,3,1,3" , globalOpCounters.getUpdate() ) );
            _callbacks.push_back( new callbacks::AtomicWordCallback( "globalOpInsert" , "1,3,1,4" , globalOpCounters.getDelete() ) );
            _callbacks.push_back( new callbacks::AtomicWordCallback( "globalOpInsert" , "1,3,1,5" , globalOpCounters.getGetMore() ) );

            // register all callbacks
            for ( unsigned i=0; i<_callbacks.size(); i++ )
                _checkRegister( _callbacks[i]->init() );
            
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
                if ( cb ) {
                    cb->respond( var );
                }
                else {
                    warning() << "no callback for: " << oidManager.toString( var->name ) << endl;
                }
                return SNMP_ERR_NOERROR;
            }

            case MODE_GETNEXT: {
                /*
                  not sure where this came from or if its remotely correct
                if ( snmpAgent._uptime == var ) {
                    snmp_set_var_objid(var,snmpAgent._uptime.getoid() , snmpAgent._uptime.len() );
                    snmp_set_var_typed_value(var, ASN_TIMETICKS, (u_char *) &uptime, sizeof(uptime) );
                    return SNMP_ERR_NOERROR;
                }
                */
                warning() << "i have no idea what i'm supposed to do with MODE_GETNEXT " << __FILE__ << ":" << __LINE__ << endl;
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

