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

import json

class SmvMetaData(object):
    def __init__(self):
        self._metadata = {
            '_fqn': '',
            '_verHex': '',
            '_timestamp': '',
            '_applicationId': '',
            '_duration': {},
            '_columns': [],
            '_inputs': [],
            '_dqmValidation': {},
            '_smvConfig': {},
            '_sparkConfig': [],
            '_sparkVersion': '',
            '_userMetadata': {},
            '_edd': []
        }

    def addFQN(self, fqn):
        self._metadata.update({'_fqn': fqn})

    def addVerHex(self, ver_hex):
        self._metadata.update({'_verHex': ver_hex})

    def addTimestamp(self, dt):
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        self._metadata.update({'_timestamp': dt_str})

    def addSchemaMetadata(self, df):
        if (df is not None):
            columns = json.loads(df.schema.json())['fields']
            self._metadata.update({'_columns': columns})

    def addDependencyMetadata(self, deps):
        paths = [m._meta_path() for m in deps]
        self._metadata.update({'_inputs': paths})

    def addDuration(self, name, duration):
        self._metadata['_duration'].update({name: duration})

    def addApplicationContext(self, smvApp):
        sc = smvApp.sc
        self._metadata.update({'_applicationId': sc.applicationId})
        self._metadata.update({'_sparkVersion': sc.version})
        self._metadata.update({'_sparkConfig': dict(sc.getConf().getAll())})

        conf  = smvApp.py_smvconf
        conf_meta = {
            'smvAppDir': conf.app_dir
        }
        conf_meta.update(conf.all_data_dirs())
        conf_meta.update(conf.merged_props())

        self._metadata.update({'_smvConfig': conf_meta})

    def addSystemMeta(self, mod):
        self.addFQN(mod.fqn())
        self.addVerHex(mod._ver_hex())
        self.addTimestamp(mod.timestamp)
        self.addApplicationContext(mod.smvApp)
        self.addDependencyMetadata(mod.resolvedRequiresDS)

    def addUserMeta(self, user_meta):
        self._metadata.update({'_userMetadata': user_meta})

    def addDqmValidationResult(self, result_json):
        self._metadata.update({'_dqmValidation': json.loads(result_json)})

    def addEddResult(self, edd_result_json_list):
        edd_list = []
        for rc in edd_result_json_list:
            edd_list.append(json.loads(rc))
        self._metadata.update({'_edd': edd_list})

    def getEddResult(self):
        return self._metadata.get('_edd')

    def toJson(self):
        return json.dumps(self._metadata)

    def fromJson(self, meta_json):
        self._metadata = json.loads(meta_json)
        return self

    def __repr__(self):
        # So that you can print(metadata)
        return json.dumps(self._metadata,
            sort_keys=True, indent=2, separators=(',', ': '))


class SmvMetaHistory(object):
    def __init__(self):
        self._hist_list = []

    def update(self, new_meta, max_size):
        self._hist_list.insert(0, new_meta._metadata)
        self._hist_list = self._hist_list[0:max_size]

    def toJson(self):
        return json.dumps({'history':self._hist_list})

    def fromJson(self, hist_json):
        hist_dict = json.loads(hist_json)
        self._hist_list = hist_dict['history']
        return self

    def __repr__(self):
        # So that you can print(metadata)
        return json.dumps(self._hist_list,
            sort_keys=True, indent=2, separators=(',', ': '))
