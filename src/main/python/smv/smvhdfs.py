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
class SmvHDFS(object):
    def __init__(self, j_smvHDFS):
        self.j_smvHDFS = j_smvHDFS

    def writeToFile(self, py_fileobj, file_name):
        out = self.j_smvHDFS.openForWrite(file_name)
        maxsize = 8192
        def read():
            buf = py_fileobj.read(maxsize)
            # The following should work in both Python 2.7 and 3.5.
            #
            # In 2.7, read() returns a str even in 'rb' mode, but calling
            # bytearray converts it to the right type.
            #
            # In 3.5, read() returns a bytes in 'rb' mode, and calling
            # bytearray does not require a specified encoding
            buf = bytearray(buf)
            return buf

        try:
            buf = read()
            while (len(buf) > 0):
                out.write(buf, 0, len(buf))
                buf = read()
        finally:
            out.close()
            py_fileobj.close()
        

 