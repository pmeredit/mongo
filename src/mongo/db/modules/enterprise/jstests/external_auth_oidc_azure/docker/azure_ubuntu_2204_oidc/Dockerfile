FROM ubuntu:latest
ARG DEBIAN_FRONTEND=noninteractive
ARG PUBKEY
RUN test -n "${PUBKEY}" || (echo "--build-arg PUBKEY must be set" && false)
RUN apt update && apt install -y openssh-server binutils curl python3 python3-pip
RUN pip install requests

RUN useradd -rm -d /home/ubuntu -s /bin/bash -u 1000 ubuntu
RUN mkdir -p /home/ubuntu/.ssh
RUN chmod 700 /home/ubuntu/.ssh

RUN echo "${PUBKEY}" > /home/ubuntu/.ssh/authorized_keys
COPY token_server.py /home/ubuntu/token_server.py
COPY entrypoint.bash /home/ubuntu/entrypoint.bash

RUN chmod 600 /home/ubuntu/.ssh/authorized_keys
RUN chown -R ubuntu /home/ubuntu

RUN service ssh start

EXPOSE 22

CMD ["bash", "/home/ubuntu/entrypoint.bash"]
