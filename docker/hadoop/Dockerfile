# NOTE: Due to issues with the base image, images built from this Dockerfile will not run properly
# on Docker with the overlay2 FS. Issues with overlay2 manifest in complaints from Rpmdb concluding
# in a corrupted Rpmdb and a broken yum. Switching to aufs resolves the issue.
FROM sequenceiq/hadoop-docker:2.7.1

ENV PATH="/root/.pyenv/bin/:$PATH"
# Spark 2.1+ requires Java 8
ENV JAVA_HOME="/usr/lib/jvm/jre-1.8.0-openjdk.x86_64/"

RUN yum -y install git zlib-devel openssl-devel java-1.8.0-openjdk-devel && \
    yum -y upgrade curl nss && \
    curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash && \
    pyenv update && \
    pyenv install 2.7.10 && pyenv global 2.7.10 && eval "$(pyenv init -)" && \
    pip install decorator && \
    rm -rf /usr/java/jdk1.7.0_71 /usr/java/default && ln -s "$JAVA_HOME" /usr/java/default && \
    sed -i '$ i\ <property><name>yarn.nodemanager.pmem-check-enabled</name><value>false</value></property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>' /usr/local/hadoop/etc/hadoop/yarn-site.xml

