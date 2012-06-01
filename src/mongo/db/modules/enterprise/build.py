
customIncludes = True

def configure( conf , env , serverOnlyFiles ):
    root = __file__
    root = root.rpartition( "/" )[0]

    gotSNMP = False

    if conf.CheckCXXHeader( "net-snmp/net-snmp-config.h" ):

        snmplibs = [ "netsnmp" + x for x in [ "" , "helpers" ] ]

        for x in snmplibs:
            if conf.CheckLib(x):
                gotSNMP = True

    if gotSNMP:
        serverOnlyFiles += env.Glob( root + "/src/snmp*.cpp" )
        env.Append( CPPDEFINES=["NETSNMP_NO_INLINE"] )
    else:
        print( "WARNING: couldn't find snmp pieces, not building snmp support" )
        
    if "installSetup" in env:
        env["installSetup"].bannerDir = root + "/distsrc"

