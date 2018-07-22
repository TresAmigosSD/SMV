ARG SMV_BASE_IMAGE
FROM ${SMV_BASE_IMAGE}

ENV TEMPLATE_DIR=/home/template

# create the projects directory and create a flag (.docker) to indicate project is inside docker image.
RUN mkdir /projects &&\
    touch /projects/.docker &&\
    mkdir ${TEMPLATE_DIR} &&\
    mkdir -p /usr/lib/SMV

ARG SMV_BUNDLE
COPY ${SMV_BUNDLE} .

RUN tar xzvf ${SMV_BUNDLE} -C /usr/lib/SMV &&\
    rm -rf ${SMV_BUNDLE} &&\
    /usr/lib/SMV/tools/spark-install --target-dir /usr/lib/spark

COPY ./kernel.json /usr/local/share/jupyter/kernels/smv-pyshell/kernel.json
COPY ./hive-site.xml /usr/lib/spark/conf/hive-site.xml
COPY ./entrypoint.sh /usr/bin/entrypoint.sh
# copy files into template directory.  They will be copied to smv home later in entrypoint.sh
COPY ./bashrc ./jupyter_notebook_config.py ${TEMPLATE_DIR}/

# Must use bracket syntax (["command"]) so that user can supply params (additional commands to execute)
ENTRYPOINT ["entrypoint.sh"]
