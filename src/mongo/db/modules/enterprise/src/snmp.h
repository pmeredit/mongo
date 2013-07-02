/*
 * Copyright (C) 2013 10gen, Inc.  All Rights Reserved.
 */

#include "pch.h"

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
        oid* getoid( string suffix );


        unsigned len( string suffix );

        string toString( oid* o );
        
    private:
        vector<oid> _root;
        vector<oid> _endName;

        // these don't get deleted now
        // its a bit annoying b/c i cache them, etc...
        map<string,oid*> _oids; 

    };

    extern OIDManager oidManager;

    /**
     * our represntation of an oid
     */
    class SOID {
    public:
        SOID( const string& suffix );

        bool operator==( const netsnmp_variable_list *var ) const;
        
        oid * getoid() const { return _oid; }
        int len() const { return _len; }

    private:
        string _suffix;
        oid * _oid;
        unsigned _len;
    };

}
