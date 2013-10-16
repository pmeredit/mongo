/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

#include <map>
#include <string>
#include <vector>

// The following headers must be in this order
#include <net-snmp/net-snmp-config.h>
#include <net-snmp/net-snmp-includes.h>
#include <net-snmp/agent/net-snmp-agent-includes.h>

namespace mongo {
    /**
     * does mapping from mongo specific suffixes
     * to full oids
     * also some caching for performance
     */
    class OIDManager {
    public:
        
        OIDManager();

        void init();
        
        /**
           eg. suffix = 1,1,1
         */
        oid* getoid( std::string suffix );


        unsigned len( std::string suffix );

        std::string toString( oid* o );
        
    private:
        std::vector<oid> _root;
        std::vector<oid> _endName;

        // these don't get deleted now
        // its a bit annoying b/c i cache them, etc...
        std::map<std::string,oid*> _oids;

    };

    extern OIDManager oidManager;

    /**
     * our represntation of an oid
     */
    class SOID {
    public:
        SOID( const std::string& suffix );

        bool operator==( const netsnmp_variable_list *var ) const;
        
        oid * getoid() const { return _oid; }
        int len() const { return _len; }

    private:
        std::string _suffix;
        oid * _oid;
        unsigned _len;
    };

}
