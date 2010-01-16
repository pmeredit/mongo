// snmp.cpp

#ifdef _HAVESNMP

#include "stdafx.h"
#include "util/background.h"
#include "db/module.h"
#include "db/dbstats.h"

#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>
#include <signal.h>

namespace mongo {

    static oid rootOID[] =
        { 1, 3, 6, 1, 4, 1, 37601 };

    int my_snmp_callback( netsnmp_mib_handler *handler, netsnmp_handler_registration *reginfo,
                          netsnmp_agent_request_info *reqinfo, netsnmp_request_info *requests);


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

            for ( uint i=0; i<sizeof(rootOID)/sizeof(oid); i++ ){
                _root.push_back( rootOID[i] );
            }
        }
        
        ~SNMPAgent(){
        }

        void config( program_options::variables_map& params ){
            if ( params.count( "snmp-subagent" ) ){
                enable();
            }
            if ( params.count( "snmp-master" ) ){
                makeMaster();
                enable();
            }
        }
    
        void enable(){
            _enabled = 1;
        }
        
        void makeMaster(){
            _subagent = false;
        }

        void init(){
            go();
        }

        void shutdown(){
            _enabled = 0;
        }
        
        void run(){
            if ( ! _enabled ){
                log(1) << "SNMPAgent not enabled" << endl;
                return;
            }
            
            snmp_enable_stderrlog();
            
            if ( _subagent ){
                if ( netsnmp_ds_set_boolean(NETSNMP_DS_APPLICATION_ID, NETSNMP_DS_AGENT_ROLE, 1) != SNMPERR_SUCCESS ){
                    log() << "SNMPAgent faild setting subagent" << endl;
                    return;
                }
            }
            
            init_agent( _agentName.c_str() );
            
            _init();
            log(1) << "SNMPAgent num things: " << _numThings << endl;
            
            init_snmp( _agentName.c_str() );
            
            if ( ! _subagent ){
                init_master_agent(); 
                log() << "SNMPAgent running as master" << endl;
            }
            else {
                log() << "SNMPAgent running as subagent" << endl;
            }
            
            while( _enabled && ! inShutdown() ){
                _snmpIterations++;
                agent_check_and_process(1);
            }
            
            log() << "SNMPAgent shutting down" << endl;        
            snmp_shutdown( _agentName.c_str() );
            SOCK_CLEANUP;
        }
        
        /**
           eg. suffix = 1,1,1
         */
        oid* getoid( string suffix ){
            oid*& it = _oids[suffix];
            if ( it )
                return it;
            
            vector<oid> l;
            for ( uint i=0; i<_root.size(); i++ )
                l.push_back( _root[i] );
            
            string::size_type pos;
            while ( ( pos = suffix.find( ',' ) ) != string::npos ){
                string x = suffix.substr( 0 , pos );
                suffix = suffix.substr( pos + 1 );
                l.push_back( atoi( x.c_str() ) );
            }
            l.push_back( atoi( suffix.c_str() ) );
            
            it = new oid[l.size()+1];

            for ( uint i=0; i<l.size(); i++ ){
                it[i] = l[i];
            }
            it[l.size()] = 0;
            return it;
        }

        int oidlen( string suffix ){
            oid* o = getoid( suffix );
            int x = 0;
            while ( o[x] )
                x++;
            return x;
        }

        string toString( oid* o ){
            stringstream ss;
            int x=0;
            while ( o[x] )
                ss << "." << o[x++];
            return ss.str();
        }

    private:
        
        void _checkRegister( int x ){
            if ( x == MIB_REGISTERED_OK ){
                _numThings++;
                return;
            }
            
            if ( x == MIB_REGISTRATION_FAILED ){
                log() << "SNMPAgent MIB_REGISTRATION_FAILED!" << endl;
            }
            else if ( x == MIB_DUPLICATE_REGISTRATION ){
                log() << "SNMPAgent MIB_DUPLICATE_REGISTRATION!" << endl;
            }
            else {
                log() << "SNMPAgent unknown registration failure" << endl;
            }
            
        }

        void _initCounter( const char * name , const char* oidhelp , int * counter ){
            log(2) << "registering: " << name << " " << toString( getoid( oidhelp ) ) << endl;

            netsnmp_handler_registration * reg = netsnmp_create_handler_registration( name , NULL,
                                                                                      getoid( oidhelp ) , oidlen( oidhelp ) , 
                                                                                      HANDLER_CAN_RONLY);
            
            netsnmp_watcher_info * winfo = netsnmp_create_watcher_info( counter, sizeof(int),
                                                                        ASN_COUNTER, WATCHER_FIXED_SIZE);
            
            _checkRegister( netsnmp_register_watched_scalar( reg, winfo ) );
        }

        void _init(){
        
            // uptime
            _startTime = curTimeMicros64();
            _uptime = "1,1,1";
            netsnmp_handler_registration * upreg = netsnmp_create_handler_registration( "sysUpTime", &my_snmp_callback ,
                                                                                        getoid( _uptime ),oidlen( _uptime ),
                                                                                        HANDLER_CAN_RONLY);
            _checkRegister( netsnmp_register_instance( upreg ) );
            
            // globalOpCounters
            _initCounter( "globalOpInsert","1,2,1,1" , globalOpCounters.getInsert() );
            _initCounter( "globalOpQuery","1,2,1,2" , globalOpCounters.getQuery() );
            _initCounter( "globalOpUpdate","1,2,1,3" , globalOpCounters.getUpdate() );
            _initCounter( "globalOpDelete","1,2,1,4" , globalOpCounters.getDelete() );
            _initCounter( "globalOpGetMore","1,2,1,5" , globalOpCounters.getGetGore() );
            
        }
        
        string _agentName;
        
        bool _enabled;
        bool _subagent;

        int _numThings;
        int _snmpIterations;

        vector<oid> _root;
        
        map<string,oid*> _oids;
        
    public:
        string _uptime;
        unsigned long long _startTime;
    } snmpAgent;

    int my_snmp_callback( netsnmp_mib_handler *handler, netsnmp_handler_registration *reginfo,
                          netsnmp_agent_request_info *reqinfo, netsnmp_request_info *requests){
        
        int uptime = ( curTimeMicros64() - snmpAgent._startTime ) / 10000;
                
        while (requests) {
            netsnmp_variable_list *var = requests->requestvb;

            switch (reqinfo->mode) {
            case MODE_GET:{
                if (netsnmp_oid_equals(var->name, var->name_length, snmpAgent.getoid( snmpAgent._uptime ) , snmpAgent.oidlen( snmpAgent._uptime ) ) == 0) {
                    snmp_set_var_typed_value(var, ASN_TIMETICKS, (u_char *) &uptime, sizeof(uptime) );
                }
                return SNMP_ERR_NOERROR;
            }
                
            case MODE_GETNEXT: {
                if (netsnmp_oid_equals(var->name, var->name_length, snmpAgent.getoid( snmpAgent._uptime ) , snmpAgent.oidlen( snmpAgent._uptime ) ) < 0 ){
                    snmp_set_var_objid(var,snmpAgent.getoid( snmpAgent._uptime ) , snmpAgent.oidlen( snmpAgent._uptime ) );
                    snmp_set_var_typed_value(var, ASN_TIMETICKS, (u_char *) &uptime, sizeof(uptime) );
                    return SNMP_ERR_NOERROR;
                }
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

#endif
