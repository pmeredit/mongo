// snmp.cpp

#ifdef _HAVESNMP

#include "stdafx.h"
#include "util/background.h"
#include "db/module.h"

#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>
#include <signal.h>

namespace mongo {

    static oid myoid[] =
        { 1, 3, 6, 1, 4, 1, 37601, 1, 1, 1 };
    static u_long myvalue = 18;

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
            
            if ( ! _subagent )
                init_master_agent(); 
            
            log() << "SNMPAgent running" << endl;
            
            while( _enabled && ! inShutdown() ){
                _snmpIterations++;
                agent_check_and_process(1);
            }
            
            log() << "SNMPAgent shutting down" << endl;        
            snmp_shutdown( _agentName.c_str() );
            SOCK_CLEANUP;
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

        void _init(){

            _checkRegister( netsnmp_register_read_only_counter32_instance( "asdasd" , 
                                                                           myoid , OID_LENGTH( myoid ) ,
                                                                           &myvalue , NULL ) );
        }

        string _agentName;
        
        bool _enabled;
        bool _subagent;

        int _numThings;
        int _snmpIterations;

    } snmpAgent;

}

#endif
