#!/bin/bash
# Program:
#       Set env on gcp auto.
# History:
# 2020/07/12  BenLiu    First release
cd ~
sudo yum remove docker \
                  docker-client \
                  docker-client-latest \
                  docker-common \
                  docker-latest \
                  docker-latest-logrotate \
                  docker-logrotate \
                  docker-engine
sudo yum install -y yum-utils
sudo yum-config-manager \
    --add-repo \
    https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install -y docker-ce docker-ce-cli containerd.io
yum list docker-ce --showduplicates | sort -r
sudo yum install -y docker-ce-<VERSION_STRING> docker-ce-cli-<VERSION_STRING> containerd.io
sudo systemctl start docker
sudo curl -L "https://github.com/docker/compose/releases/download/1.26.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose


echo 'docker env. is ready to run.'
# setting env. on docker
# method one:git clone
# method two:sh
