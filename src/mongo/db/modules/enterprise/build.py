
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

    # v2.0 builds don't support module tests
    try:
        from buildscripts import moduleconfig
    except ImportError:
        print( "WARNING: v2.0 and older builds don't support module tests, skipping" )
    else:
        # just a simple example -- this will always pass
        moduleconfig.register_module_test('/bin/true')

