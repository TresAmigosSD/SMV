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

import sys

class ExtraPath(object):
    def __init__(self, extra_path):
        self.extra_path = extra_path

    def __enter__(self):
        sys.path.insert(1, self.extra_path)

    def __exit__(self, type, value, traceback):
        sys.path.remove(self.extra_path)

class AppDir(object):
    """temporary set of application dir during testing"""
    def __init__(self, smvApp, app_path):
        self.smvApp = smvApp
        self.app_path = app_path

    def __enter__(self):
        self.orig_app_path = self.smvApp.appDir()
        # In case setAppDir throws error, reset appdir back to original
        try:
            self.smvApp.setAppDir(self.app_path)
        except Exception as e:
            self.smvApp.setAppDir(self.orig_app_path)
            raise e


    def __exit__(self, type, value, traceback):
        self.smvApp.setAppDir(self.orig_app_path)
