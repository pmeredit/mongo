#! /bin/bash
python3 /home/ubuntu/token_server.py &
/usr/sbin/sshd -D &
wait -n
exit $?
