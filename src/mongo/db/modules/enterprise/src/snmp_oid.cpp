// snmp_oid.cpp

#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>

#include "snmp.h"

#include "db/cmdline.h"

namespace mongo {
    OIDManager oidManager;

    static oid rootOID[] = { 1, 3, 6, 1, 4, 1, 37601 , 1 };

    OIDManager::OIDManager() {
        for ( uint i=0; i<sizeof(rootOID)/sizeof(oid); i++ ) {
            _root.push_back( rootOID[i] );
        }
    }
    
    void OIDManager::init() {
        char buf[128];
        int x = sprintf( buf , "%d" , cmdLine.port );
        _endName.push_back( (oid)x );
        for ( int i=0; i<x; i++ ) {
            _endName.push_back( (oid)buf[i] );
        }
    }
    
    oid* OIDManager::getoid( string suffix ) {
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
        
        for ( uint i=0; i<_endName.size(); i++ )
            l.push_back( _endName[i] );

        it = new oid[l.size()+1];
        
        for ( uint i=0; i<l.size(); i++ ) {
            it[i] = l[i];
        }
        it[l.size()] = 0;
        return it;
    }
    
    unsigned OIDManager::len( string suffix ) {
        oid* o = getoid( suffix );
        unsigned x = 0;
        while ( o[x] )
            x++;
        return x;
    }
    
    string OIDManager::toString( oid* o ) {
        stringstream ss;
        int x=0;
        while ( o[x] )
            ss << "." << o[x++];
        return ss.str();
    }
    

    SOID::SOID( const string& suffix ) : _suffix( suffix ) {
        _oid = oidManager.getoid( _suffix );
        _len = oidManager.len( _suffix );
    }

    bool SOID::operator==( const netsnmp_variable_list *var ) const {
        if ( _len != var->name_length )
            return false;
        
        for ( unsigned i=0; i<_len; i++ ) 
            if ( _oid[i] != var->name[i] )
                return false;
        
        return true;
    }
    
}
