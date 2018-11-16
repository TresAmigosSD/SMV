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
import sys
import itertools
import pkgutil
from smv.error import SmvRuntimeError

def lazy_property(fn):
    '''Decorator that makes a property lazy-evaluated.
    '''
    attr_name = '_lazy_' + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)
    return _lazy_property

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
    elif (isinstance(elem, DataFrame)):
        jcols = sc._gateway.new_array(sc._jvm.org.apache.spark.sql.Dataset, len(cols))
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

def scala_seq_to_list(_jvm, j_seq):
    """Convert Scala Seq to Python list
    """
    j_list = _jvm.scala.collection.JavaConversions.seqAsJavaList(j_seq)
    return [x for x in j_list]

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

def list_distinct(l):
    """Return a the distinct version of the input list, perserve order
    """
    uniq_list = []
    seen = set()
    for x in l:
        if x not in seen:
            seen.add(x)
            uniq_list.append(x)
    return uniq_list

def infer_full_name_from_part(full_names, part_name):
    """For a given partial name (postfix), infer full name from a list
    """
    candidates = [s for s in full_names if s.endswith(part_name)]

    if (len(candidates) == 0):
        raise SmvRuntimeError("Can't find name {}".format(part_name))
    elif(len(candidates) == 1):
        return candidates[0]
    else:
        raise SmvRuntimeError("Partial name {} is ambiguous".format(part_name))

# If using Python 2, prefer cPickle because it is faster
# If using Python 3, there is no cPickle (cPickle is now the implementation of pickle)
# see https://docs.python.org/3.1/whatsnew/3.0.html#library-changes
try:
    pickle_lib = __import__("cPickle")
except ImportError:
    pickle_lib = __import__("pickle")


def smvhash(text):
    """Python's hash function will return different numbers from run to
    from, starting from 3.  Provide a deterministic hash function for
    use to calculate sourceCodeHash.
    """
    import binascii

    # Python 2* has "str" type the same as bytes, while Python 3 has
    # to covert "str" to bytes through "str".encode("utf-8")
    if sys.version_info >= (3, 0):
        byte_str = text.encode("utf-8")
    else:
        byte_str = text

    return binascii.crc32(byte_str)
