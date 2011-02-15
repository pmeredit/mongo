
Master
---------

To run as master, you need to put a config file in /etc/snmp/mongod.conf
You can use the sample here.

sudo ln -s `pwd`/MONGO-MIB.txt //usr/share/snmp/mibs/

snmpwalk -m MONGO-MIB -v 2c -c mongodb 127.0.0.1:1161 1.3.6.1.4.1.37601

