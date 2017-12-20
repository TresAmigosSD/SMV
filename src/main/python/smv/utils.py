#
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql.column import Column
from pyspark.sql import DataFrame
import itertools
import pkgutil

def iter_submodules(stages):
    """Yield the names of all submodules of the packages corresponding to the given stages
    """
    file_iters_by_stage = (iter_submodules_in_stage(stage) for stage in stages)
    file_iter = itertools.chain(*file_iters_by_stage)
    return (name for (_, name, is_pkg) in file_iter if not is_pkg)

def iter_submodules_in_stage(stage):
    """Yield info on the submodules of the package corresponding with a given stage
    """
    try:
        stagemod = __import__(stage)
    except:
        return []
    # `walk_packages` can generate AttributeError if the system has
    # Gtk modules, which are not designed to use with reflection or
    # introspection. Best action to take in this situation is probably
    # to simply suppress the error.
    def onerror(): pass
    return pkgutil.walk_packages(stagemod.__path__, stagemod.__name__ + '.' , onerror=onerror)

def for_name(name, stages):
    """Dynamically load a module in a stage by its name.

        Similar to Java's Class.forName, but only looks in configured stages.
    """
    lastdot = name.rfind('.')
    file_name = name[ : lastdot]
    mod_name = name[lastdot+1 : ]

    mod = None

    # if file doesnt exist, module doesn't exist
    if file_name in iter_submodules(stages):
        # __import__ instantiates the module hierarchy but returns the root module
        f = __import__(file_name)
        # iterate to get the file that should contain the desired module
        for subname in file_name.split('.')[1:]:
            f = getattr(f, subname)
        # leave mod as None if the file exists but doesnt have an attribute with tha name
        if hasattr(f, mod_name):
            mod = getattr(f, mod_name)

    return mod


def smv_copy_array(sc, *cols):
    """Copy Python list to appropriate Java array
    """
    if (len(cols) == 0):        # may need to pass the correct java type somehow
        return sc._gateway.new_array(sc._jvm.java.lang.String, 0)

    elem = cols[0]
    if is_string(elem):
        jcols = sc._gateway.new_array(sc._jvm.java.lang.String, len(cols))
        for i in range(0, len(jcols)):
            jcols[i] = cols[i]
    elif isinstance(elem, Column):
        jcols = sc._gateway.new_array(sc._jvm.org.apache.spark.sql.Column, len(cols))
        for i in range(0, len(jcols)):
            jcols[i] = cols[i]._jc
    elif isinstance(elem, DataFrame):
        jcols = sc._gateway.new_array(sc._jvm.org.apache.spark.sql.DataFrame, len(cols))
        for i in range(0, len(jcols)):
            jcols[i] = cols[i]._jdf
    elif isinstance(elem, list): # a list of list
        # use Java List as the outermost container; an Array[Array]
        # will not always work, because the inner list may be of
        # different lengths
        jcols = sc._jvm.java.util.ArrayList()
        for i in range(0, len(cols)):
            jcols.append(smv_copy_array(sc, *cols[i]))
    elif isinstance(elem, tuple):
        jcols = sc._jvm.java.util.ArrayList()
        for i in range(0, len(cols)):
            # Use Java List for tuple
            je = sc._jvm.java.util.ArrayList()
            for e in cols[i]: je.append(e)
            jcols.append(je)
    else:
        raise RuntimeError("Cannot copy array of type", type(elem))

    return jcols

def check_socket(port):
    """Check whether the given port is open to bind"""
    import socket
    from contextlib import closing

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        try:
            sock.bind(('', port))
        except:
            #Port is not open
            res = False
        else:
            #Port is open
            res = True

    return res

def is_string(obj):
    """Check whether object is a string type with Python 2 and Python 3 compatibility
    """
    # See http://www.rfk.id.au/blog/entry/preparing-pyenchant-for-python-3/
    try:
        return isinstance(obj, basestring)
    except:
        return isinstance(obj, str)

class FileObjInputStream(object):
    """Wraps a Python binary file object to be used like a java.io.InputStream."""

    def __init__(self, fileobj):
        self.fileobj = fileobj

    def read(self, maxsize):
        buf = self.fileobj.read(maxsize)
        if isinstance(buf, basestring): # we will get a string back if we are reading a text file
            buf = bytearray(buf)
        return buf

    def close(self):
        self.fileobj.close()

    class Java:
        implements = ['org.tresamigos.smv.IAnyInputStream']

# If using Python 2, prefer cPickle because it is faster
# If using Python 3, there is no cPickle (cPickle is now the implementation of pickle)
# see https://docs.python.org/3.1/whatsnew/3.0.html#library-changes
try:
    pickle_lib = __import__("cPickle")
except ImportError:
    pickle_lib = __import__("pickle")
