
customIncludes = True

def configure( conf , env , serverOnlyFiles ):
    
    gotSNMP = False

    if conf.CheckCXXHeader( "net-snmp/net-snmp-config.h" ):

        snmplibs = [ "netsnmp" + x for x in [ "" ] ]

        gotAll = True
        for x in snmplibs:
            if not conf.CheckLib(x):
                gotAll = False
        if gotAll:
            gotSNMP = True
        else:
            for x in snmplibs:
                removeIfInList( env["LIBS"] , x )

    if gotSNMP:
        serverOnlyFiles += env.Glob( "db/modules/enterprise/src/snmp*.cpp" )
        env.Append( CPPDEFINES=["NETSNMP_NO_INLINE"] )
    else:
        print( "WARNING: couldn't find all snmp pieces, not building snmp support" )
