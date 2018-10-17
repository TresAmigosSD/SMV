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

from datetime import datetime

class DataSetResolver:
    def __init__(self, repo):
        self.repo = repo
        self.fqn2res = {}
        self.resolveStack = []
        self.transaction_time = datetime.now()

    def loadDataSet(self, fqns):
        res = []
        for fqn in fqns:
            # Caller need to check whether the urn is in a stage of the SmvConfig stages
            ds = self.fqn2res.get(fqn,
                self.resolveDataSet(self.repo.loadDataSet(fqn))
            )
            res.append(ds)
        return res

    def resolveDataSet(self, ds):
        if (ds in self.resolveStack):
            raise SmvRuntimeError("Cycle found while resolving {}: {}".format(ds.fqn(), ", ".join(self.resolveDataSet)))
        else:
            if (ds.fqn() in self.fqn2res):
                return self.fqn2res.get(ds.fqn())
            else:
                self.resolveStack.append(ds.fqn())
                resolvedDs = ds.resolve(self)
                resolvedDs.setTimestamp(self.transaction_time)
                self.fqn2res.update({ds.fqn(): resolvedDs})
                self.resolveStack = self.resolveStack[1:]
                return resolvedDs

    