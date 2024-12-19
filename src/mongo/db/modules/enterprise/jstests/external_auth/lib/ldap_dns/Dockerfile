#ifdef UBUNTU
FROM ubuntu:18.04
RUN apt-get update && apt-get install -y curl dnsmasq dnsutils

#elif defined(RHEL)

# Per https://developers.redhat.com/blog/2020/03/24/red-hat-universal-base-images-for-docker-users#introducing_red_hat_universal_base_images
FROM registry.access.redhat.com/ubi8/ubi:latest
RUN dnf update -y --nobest && dnf install -y dnsmasq libatomic bind-utils

#else

FROM fedora:36
RUN dnf update -y && dnf install -y dnsmasq libatomic bind-utils

#endif


# Install a config file that describes the DNS settings to use
COPY dnsmasq.conf /root

# Create /data/db for mongod to use
RUN mkdir -p /data/db

# Put mongo binaries in the path
# /app/build is for developer machines, /app/dist-test is for Evergreen machines
ENV PATH="/app/build/install/bin:/app/dist-test/bin:${PATH}"

# dnsmasq 2.79 in Ubuntu 18.04 - If a CNAME points to a record not known by dnsmasq, it sends the CNAME to the external server
# dnsmasq considers it known if it is /etc/hosts
# dnsmasq 2.86 in Fedora 36 - If a CNAME points to a record not know by dnsmasq, it does not send the CNAME to the external server
# Also, we add entries to /etc/hosts at runtime since docker overwrites /etc/hosts on container start and ignores /etc/hosts from the image
#
CMD ["/bin/sh", "-c" , "echo 54.225.237.121 ldaptest.10gen.cc  >> /etc/hosts && exec /usr/sbin/dnsmasq -C /root/dnsmasq.conf"]
