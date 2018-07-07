FROM tresamigos/smv:v2.3.0.0

ENV PATH=$PATH:/root/.pyenv/bin

RUN apt-get update && apt-get install -y libssl-dev && \
    curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash && \
    echo "eval \"\$(pyenv init -)\"" >> /root/.bashrc && \
    pyenv install 2.7.13 && \
    pyenv install 3.5.3 && \
    pyenv install 3.6.5 && \
    pyenv global 3.6.5 3.5.3 2.7.13 && \
    pip install tox

ENTRYPOINT []