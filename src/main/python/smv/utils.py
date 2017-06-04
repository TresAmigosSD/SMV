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

def for_name(name):
    """Dynamically load a class by its name.

    Equivalent to Java's Class.forName
    """
    lastdot = name.rfind('.')

    if lastdot == -1:
        modname = name
    else:
        modname = name[:lastdot]

    mod = __import__(modname)
    for comp in name.split('.')[1:]:
        mod = getattr(mod, comp)
    return mod

def smv_copy_array(sc, *cols):
    """Copy Python list to appropriate Java array
    """
    if (len(cols) == 0):        # may need to pass the correct java type somehow
        return sc._gateway.new_array(sc._jvm.java.lang.String, 0)

    elem = cols[0]
    if (isinstance(elem, basestring)):
        jcols = sc._gateway.new_array(sc._jvm.java.lang.String, len(cols))
        for i in range(0, len(jcols)):
            jcols[i] = cols[i]
    elif (isinstance(elem, Column)):
        jcols = sc._gateway.new_array(sc._jvm.org.apache.spark.sql.Column, len(cols))
        for i in range(0, len(jcols)):
            jcols[i] = cols[i]._jc
    elif (isinstance(elem, DataFrame)):
        jcols = sc._gateway.new_array(sc._jvm.org.apache.spark.sql.Dataset, len(cols))
        for i in range(0, len(jcols)):
            jcols[i] = cols[i]._jdf
    elif (isinstance(elem, list)): # a list of list
        # use Java List as the outermost container; an Array[Array]
        # will not always work, because the inner list may be of
        # different lengths
        jcols = sc._jvm.java.util.ArrayList()
        for i in range(0, len(cols)):
            jcols.append(smv_copy_array(sc, *cols[i]))
    elif (isinstance(elem, tuple)):
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
