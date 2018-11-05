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

class SmvLock(object):
    """Create a lock context
    """
    def __init__(self, _jvm, _lock_path):
        self._jvm = _jvm
        self._lock_path = _lock_path
        self.slock = None

    def __enter__(self):
        # create a lock file with timeout as 1 hour
        self.slock = self._jvm.org.tresamigos.smv.SmvLock(
            self._lock_path,
            3600 * 1000 
        )
        self.slock.lock()
        return None

    def __exit__(self, type, value, traceback):
        self.slock.unlock()

class NonOpLock(object):
    def __enter__(self):
        pass
    def __exit__(self, type, value, traceback):
        pass