// snmp.cpp

#include "pch.h"

#include "util/background.h"
#include "util/time_support.h"

#include "db/module.h"
#include "db/stats/counters.h"

#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>
#include <signal.h>

namespace mongo {

    static oid rootOID[] =
        { 1, 3, 6, 1, 4, 1, 37601 };
    
    int my_snmp_callback( netsnmp_mib_handler *handler, netsnmp_handler_registration *reginfo,
                          netsnmp_agent_request_info *reqinfo, netsnmp_request_info *requests);

    
    /**
     * does mapping from mongo specific suffixes
     * to full oids
     * also some caching for performance
     */
    class OIDManager {
    public:
        
        OIDManager() {
            for ( uint i=0; i<sizeof(rootOID)/sizeof(oid); i++ ) {
                _root.push_back( rootOID[i] );
            }
        }
        
        /**
           eg. suffix = 1,1,1
         */
        oid* getoid( string suffix ) {
            oid*& it = _oids[suffix];
            if ( it )
                return it;

            vector<oid> l;
            for ( uint i=0; i<_root.size(); i++ )
                l.push_back( _root[i] );

            string::size_type pos;
            while ( ( pos = suffix.find( ',' ) ) != string::npos ) {
                string x = suffix.substr( 0 , pos );
                suffix = suffix.substr( pos + 1 );
                l.push_back( atoi( x.c_str() ) );
            }
            l.push_back( atoi( suffix.c_str() ) );

            it = new oid[l.size()+1];

            for ( uint i=0; i<l.size(); i++ ) {
                it[i] = l[i];
            }
            it[l.size()] = 0;
            return it;
        }

        unsigned len( string suffix ) {
            oid* o = getoid( suffix );
            unsigned x = 0;
            while ( o[x] )
                x++;
            return x;
        }

        string toString( oid* o ) {
            stringstream ss;
            int x=0;
            while ( o[x] )
                ss << "." << o[x++];
            return ss.str();
        }

        
    private:
        vector<oid> _root;

        // these don't get deleted now
        // its a bit annoying b/c i cache them, etc...
        map<string,oid*> _oids; 

    } oidManager;

    /**
     * our represntation of an oid
     */
    class SOID {
    public:
        SOID( const string& suffix ) : _suffix( suffix ) {
            _oid = oidManager.getoid( _suffix );
            _len = oidManager.len( _suffix );
        }

        bool operator==( const netsnmp_variable_list *var ) const {
            if ( _len != var->name_length )
                return false;
            
            for ( unsigned i=0; i<_len; i++ ) 
                if ( _oid[i] != var->name[i] )
                    return false;

            return true;
        }
        
        oid * getoid() const { return _oid; }
        int len() const { return _len; }

    private:
        string _suffix;
        oid * _oid;
        unsigned _len;
    };

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

        class UptimeCallback : public SNMPCallBack {
        public:
            UptimeCallback() : SNMPCallBack( "sysUpTime" , "1,1,1" ) {
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
                : SNMPCallBack( name , "1,3," + memsuffix ) , _type(t) {
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

        void config( program_options::variables_map& params ) {
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
            go();
        }

        void shutdown() {
            _enabled = 0;
        }

        void run() {
            
            while ( ! _enabled ) {
                log(1) << "SNMPAgent not enabled" << endl;
                return;
            }

            snmp_enable_stderrlog();

            if ( _subagent ) {
                if ( netsnmp_ds_set_boolean(NETSNMP_DS_APPLICATION_ID, NETSNMP_DS_AGENT_ROLE, 1) != SNMPERR_SUCCESS ) {
                    log() << "SNMPAgent faild setting subagent" << endl;
                    return;
                }
            }

            init_agent( _agentName.c_str() );

            _init();
            log(1) << "SNMPAgent num things: " << _numThings << endl;

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
            log(2) << "registering: " << name << " " << oidhelp << endl;

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
            log(2) << "registering: " << name << " " << oidhelp << endl;

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
            callbacks::MemoryCallback::addAll( _callbacks );

            // register
            for ( unsigned i=0; i<_callbacks.size(); i++ )
                _checkRegister( _callbacks[i]->init() );
            
            // static counters
            
            //  ---- globalOpCounters
            _initCounter( "globalOpInsert" , "1,2,1,1" , globalOpCounters.getInsert() );
            _initCounter( "globalOpQuery" , "1,2,1,2" , globalOpCounters.getQuery() );
            _initCounter( "globalOpUpdate" , "1,2,1,3" , globalOpCounters.getUpdate() );
            _initCounter( "globalOpDelete" , "1,2,1,4" , globalOpCounters.getDelete() );
            _initCounter( "globalOpGetMore" , "1,2,1,5" , globalOpCounters.getGetMore() );

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

