FROM ubuntu:18.04
RUN apt-get update && apt-get install -y libsnmp-dev curl dnsmasq

# FROM fedora:34
# RUN dnf update -y && dnf install -y dnsmasq net-snmp-agent-libs libatomic

# Install a config file that describes the DNS settings to use
COPY dnsmasq.conf /root

# Create /data/db for mongod to use
RUN mkdir -p /data/db

# Put mongo binaries in the path
# /app/build is for developer machines, /app/dist-test is for Evergreen machines
ENV PATH="/app/build/install/bin:/app/dist-test/bin:${PATH}"

CMD ["/usr/sbin/dnsmasq", "-C", "/root/dnsmasq.conf"]
