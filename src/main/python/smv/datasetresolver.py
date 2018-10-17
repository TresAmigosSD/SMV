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
    """DataSetResolver (DSR) is the entrypoint through which the DataSetMgr acquires
        SmvDataSets. A DSR object represent a single transaction. Each DSR creates a
        set of DataSetRepos at instantiation. When asked for an SmvDataSet, DSR queries
        the repos for that SmvDataSet and resolves it. The SmvDataSet is responsible for
        resolving itself, given access to the DSR to load/resolve the SmvDataSet's
        dependencies. DSR caches the SmvDataSets it has already resolved to ensure that
        any SmvDataSet is only resolved once.
    """
    def __init__(self, repo):
        self.repo = repo

        # FQN to resolved SmvDataSet
        self.fqn2res = {}

        # Track which SmvDataSets is currently being resolved. Used to check for
        # dependency cycles. Note: we no longer have to worry about corruption of
        # resolve stack because a new stack is created per transaction.
        self.resolveStack = []

        #Timestamp which will be injected into the resolved SmvDataSets
        self.transaction_time = datetime.now()

    def loadDataSet(self, fqns):
        """Given a list of FQNs, return cached resolved version SmvDataSets if exists, or
            otherwise load unresolved version from source and resolve them.
        """
        res = []
        for fqn in fqns:
            # Caller need to check whether the urn is in a stage of the SmvConfig stages
            ds = self.fqn2res.get(fqn,
                self.resolveDataSet(self.repo.loadDataSet(fqn))
            )
            res.append(ds)
        return res

    def resolveDataSet(self, ds):
        """Return cached resolved version of given SmvDataSet if it exists, or resolve
            it otherwise.
        """
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

    