
def configure( conf , env ):
    
    gotSNMP = False

    if conf.CheckCXXHeader( "net-snmp/net-snmp-config.h" ):

        snmplibs = [ "netsnmp" + x for x in [ "mibs" , "agent" , "helpers" , "" ] ]

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
        env.Append( CPPDEFINES=[ "_HAVESNMP" ] )
    else:
        print( "WARNING: couldn't find all snmp pieces, not building snmp support" )
