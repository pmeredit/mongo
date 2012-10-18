import buildscripts.moduleconfig as moduleconfig
import os

def configure(conf, env):
    root = os.path.dirname(__file__)

    if conf.CheckCXXHeader( "net-snmp/net-snmp-config.h" ):
        try:
            snmpFlags = env.ParseFlags("!net-snmp-config --agent-libs")
        except OSError, ose:
            # the net-snmp-config command was not found
            print( "WARNING: could not find or execute 'net-snmp-config', is it on the PATH?" )
            print( ose )
        else:
            snmp_module_name= moduleconfig.get_current_module_libdep_name('mongosnmp')
            env['SNMP_SYSLIBDEPS'] = snmpFlags['LIBS']
            del snmpFlags['LIBS']
            env.Append(**snmpFlags)
            env.Append(CPPDEFINES=["NETSNMP_NO_INLINE"],
                       MODULE_LIBDEPS_MONGOD=snmp_module_name)

    if "installSetup" in env:
        env["installSetup"].bannerDir = root + "/distsrc"
