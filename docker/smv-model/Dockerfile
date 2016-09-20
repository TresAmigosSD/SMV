FROM tresamigos/smv-jupyter

ENV KERAS_VSN=0.2.0 \
    NUMPY_VSN=1.10.1 \
    PANDAS_VSN=0.17.1 \
    PYTHON_DATEUTIL_VSN=2.4.2 \
    SCIKIT_LEARN_VSN=0.16.1 \
    THEANO_VSN=0.7.0 \
    MATPLOTLIB_VSN=1.5.0 \
    H5PY_VSN=2.5.0 \
    FLASK_VSN=0.10.1 \
    REQUESTS_VSN=2.8.1

ARG APT_BUILD_DEP="python2.7-dev pkg-config libfreetype6-dev libhdf5-dev"
ARG PIP_BUILD_DEP="Cython"

RUN sudo apt-get update &&\
    sudo apt-get install -y $APT_BUILD_DEP &&\
    sudo pip install $PIP_BUILD_DEP &&\
    sudo pip install numpy==$NUMPY_VSN &&\
    sudo pip install pandas==$PANDAS_VSN &&\
    sudo pip install python-dateutil==$PYTHON_DATEUTIL_VSN &&\
    sudo pip install scikit-learn==$SCIKIT_LEARN_VSN &&\
    sudo pip install theano==$THEANO_VSN &&\
    sudo pip install matplotlib==$MATPLOTLIB_VSN &&\
    sudo pip install h5py==$H5PY_VSN &&\
    sudo pip install flask==$FLASK_VSN &&\
    sudo pip install requests==$REQUESTS_VSN &&\
    sudo pip install gevent &&\
    sudo pip install keras==$KERAS_VSN &&\
    sudo pip uninstall -y $PIP_BUILD_DEP &&\
    sudo apt-get autoremove --purge -y $APT_BUILD_DEP &&\
    sudo apt-get clean &&\
    sudo rm -rf /var/lib/apt/lists/* &&\
    sudo rm -rf /tmp/* /var/tmp/*
