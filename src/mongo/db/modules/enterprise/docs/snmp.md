
Master
------

To run as master, you need to put a config file in /etc/snmp/mongod.conf
You can use the sample here.

    sudo ln -s `pwd`/MONGO-MIB.txt /usr/share/snmp/mibs/

Start `mongod` with the `--snmp-master` flag, then run:

    snmpwalk -m +MONGO-MIB -v 2c -c mongodb localhost:1161 mongodb


Sub-Agent
---------

MongoDB can also run as an SNMP sub-agent. To do so, start `mongod` with the
`--snmp-subagent` flag, then run (from this directory):

    snmpwalk -M +`pwd` -m +MONGO-MIB -v 2c -c mongodb localhost mongodb

You may need to create the read-only community "mongodb" in
/etc/snmp/snmpd.conf.
