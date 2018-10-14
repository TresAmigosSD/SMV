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

from test_support.smvbasetest import SmvBaseTest
from py4j.protocol import Py4JJavaError
from smv.error import SmvRuntimeError

class RunCmdLineBaseTest(SmvBaseTest):
    @classmethod
    def whatToRun(cls):
        ["-m", "None"]

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=runstage.stage1'] + cls.whatToRun()

    

class RunModuleFromCmdLineTest(RunCmdLineBaseTest):
    @classmethod
    def whatToRun(cls):
        return ['-m', "modules.A"]

    def test_can_run_module_from_cmdline(self):
        self.smvApp.run()
        a = self.df("runstage.stage1.modules.A")
        expected = self.createDF("k:String;v:Integer", "a,;b,2")
        self.should_be_same(a, expected)

class DryRunTest(RunCmdLineBaseTest):
    @classmethod
    def whatToRun(cls):
        return ["-m", "modules.A", "--dry-run"]

    def test_dry_run_just_print(self):
        self.smvApp.run()
        self.assertTrue(self.load("runstage.stage1.modules.A")[0].needsToRun())

class RunStageFromCmdLineTest(RunCmdLineBaseTest):
    @classmethod
    def whatToRun(cls):
        return ['-s', "runstage.stage1"]

    def test_can_run_stage_from_cmdline(self):
        self.smvApp.run()
        a = self.df("runstage.stage1.modules.A")
        self.should_be_same(a, self.createDF("k:String;v:Integer", "a,;b,2"))
        b = self.df("runstage.stage1.modules.B")
        self.should_be_same(b, self.createDF("k:String;v:Integer", "c,3;d,4"))

class RunNotExistModuleTest(RunCmdLineBaseTest):
    @classmethod
    def whatToRun(cls):
        return ['-m', 'tooth-fary']

    def test_should_report_non_existing_module(self):
        with self.assertRaisesRegexp(SmvRuntimeError, "Can't find name tooth-fary"):
            self.smvApp.run()

class RunModuleAmbiguousTest(RunCmdLineBaseTest):
    @classmethod
    def whatToRun(cls):
        return ['-m', 'A']
    
    def test_should_report_ambiguous_modules(self):
        with self.assertRaisesRegexp(SmvRuntimeError, r"Partial name A is ambiguous"):
            self.smvApp.run()

class SmvAppForceAllTest(RunCmdLineBaseTest):
    @classmethod
    def whatToRun(cls):
        return ['-m', 'modules.A', '--force-run-all']

    def test_should_force_run(self):
        self.smvApp.run()
    
class SmvAppPurgeTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=runstage.stage1',
            '-m', 'modules.A',
            '--purge-old-output']

    def tmpOutputDir(self):
        return self.tmpDataDir() + "/output"

    def createTempOutputFile(self, baseName, fileContents = "xxx"):
        """create a temp file in the input data dir with the given contents"""
        import os
        fullPath = self.tmpOutputDir() + "/" + baseName
        directory = os.path.dirname(fullPath)
        if not os.path.exists(directory):
            os.makedirs(directory)

        f = open(fullPath, "w")
        f.write(fileContents)
        f.close()

    def test_old_persisted_data_should_be_removed(self):
        import os
        # create some dummy old persisted data files
        self.createTempOutputFile("runstage.stage1.modules.A_111.csv")

        self.smvApp.run()
        self.assertNotIn(
            "runstage.stage1.modules.A_111.csv", 
            os.listdir(self.tmpOutputDir())
        )


class CreateDot(RunCmdLineBaseTest):
    @classmethod
    def whatToRun(cls):
        return ['-s', "runstage.stage1", '--graph']
    def test_create_dot_graph_file(self):
        import os
        self.smvApp.run()

        dot_file = "{}.dot".format(self.smvApp.appName())
        assert (os.path.isfile(dot_file) )
        os.remove(dot_file)


class CreateEdd(RunCmdLineBaseTest):
    @classmethod
    def whatToRun(cls):
        return ['-m', 'modules.A', '--edd']

    def test_run_module_with_edd(self):
        self.smvApp.run()
        coll = self.smvApp.getRunInfoByPartialName('modules.A', None)
        edd_json_array = coll.metadata("modules.A")['_edd']
        for r in edd_json_array:
            if (r['colName'] == 'k' and r['taskDesc'] == "Non-Null Count"):
                self.assertEqual(r['valueJSON'], '2')