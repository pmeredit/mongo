
customIncludes = True

def configure( conf , env , serverOnlyFiles ):
    root = __file__
    root = root.rpartition( "/" )[0]

    if conf.CheckCXXHeader( "net-snmp/net-snmp-config.h" ):
        try:
            snmpFlags = env.ParseFlags("!net-snmp-config --agent-libs")
        except OSError, ose:
            # the net-snmp-config command was not found
            print( "WARNING: could not find or execute 'net-snmp-config', is it on the PATH?" )
            print( ose )
        else:
            env.Append( **snmpFlags )
            env.Append( CPPDEFINES=["NETSNMP_NO_INLINE"] )
            serverOnlyFiles += env.Glob( root + "/src/snmp*.cpp" )

    if "installSetup" in env:
        env["installSetup"].bannerDir = root + "/distsrc"

