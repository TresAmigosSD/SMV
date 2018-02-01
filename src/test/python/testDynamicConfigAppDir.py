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

import os
from test_support.smvbasetest import SmvBaseTest
from smv import SmvApp
import json
import sys

# TODO figure out why this kills all the successive tests
class RunModuleWithDynamicConfigAppDirTest(SmvBaseTest):
    initial_app_dir = None

    # project A stuff
    projADir = 'project-a/'
    modUrn = 'mod:stage.modules.A'
    modName = 'modules.A'

    # project B stuff
    projBDir = 'project-b/'
    diffStageUrn = 'mod:notstage.notmodules.NotModuleA'
    # same stage/name as project_a modA
    sameStageUrn = 'mod:stage.modules.A'

    def setUp(self):
        """ Setup the stuff needed for all test cases ONCE """
        # assemble proj abs paths
        path_here = os.path.dirname(os.path.realpath(__file__))
        resource_dir = self.__class__.resourceTestDir()
        self.proj_a_path = os.path.abspath(os.path.join(resource_dir, self.projADir))
        self.proj_b_path = os.path.abspath(os.path.join(resource_dir, self.projBDir))

        # capture the initial default app dir before each test. When the first test from this class
        # is run, this will be 
        self.initial_app_dir = self.smvApp.config().appDir()

    def tearDown(self):
        """ Reset the app dir to what it was initially after EACH test. Should be '.' '"""
        self.smvApp.setAppDir(self.initial_app_dir)

    @classmethod
    def tearDownClass(cls):
        # run default class teardown
        super(RunModuleWithDynamicConfigAppDirTest, cls).tearDownClass()

    def test_mods_available_to_run(self):
        """ The intention of this test is to verify that the module is initially not avaiable because the project dir is
            a child of the cwd, and then that the module is avaiable and can be run once appDir is set to the project dir """
        # make sure running the module without setting the app dir fails and raises
        err = None
        try:
            self.smvApp.runModule(self.modUrn)[0]
        except Exception as e:
            err = e

        print('--- Err type:')
        print(type(err))
        self.assertEqual(isinstance(err, Exception), True)

        # set the app dir
        self.smvApp.setAppDir(self.proj_a_path)

        # Now try to run the module. This time we expect success
        A = self.smvApp.runModule(self.modUrn)[0]
        expected = self.createDF('column:String', 'A')

        self.should_be_same(expected, A)

    def test_conf_reloaded(self):
        """ Test that app props were dynamically reloaded when appDir is set by the most direct means avaiable """
        print('--- project a path')
        print(self.proj_a_path)
        # contents of project-a/conf/smv-app-conf.props:
        expected_props = { "smv.class_dir": "./target/classes", "smv.appName": "App A", \
        "smv.config.keys": "", "smv.stages": "stage", "smv.appId": "PROJECT_A" }

        #  tell the app to change dirs, which should cause the reload of conf for this project
        self.smvApp.setAppDir(self.proj_a_path)

        current_conf = json.loads(self.smvApp.mergedPropsJSON())

        self.assertEqual(expected_props, current_conf)

    def test_run_modules(self):
        """ Ensure that we really changed out of A by verifying that a mod with same stage, name
            is actually found in its project """
        # set the app dir to project A
        self.smvApp.setAppDir(self.proj_a_path)

        # we can run a module from A, output is expected
        A = self.smvApp.runModule(self.modUrn)[0]
        expected = self.createDF('column:String', 'A')

        self.should_be_same(expected, A)

        # Now, we change to project B
        self.smvApp.setAppDir(self.proj_b_path)

        # run a module with the same name and stage but different output to ensure
        # that we really made the change
        similar_to_a = self.smvApp.runModule(self.sameStageUrn)[0]
        expected_diff_result = self.createDF('column:String', "I am Not Project A's Module A")

        self.should_be_same(similar_to_a, expected_diff_result)

        # run a module with a different stage name
        different_stage_result = self.smvApp.runModule(self.diffStageUrn)[0]
        expected_diff_stage_result = self.createDF('column:String', "Also Not Module A")

        self.should_be_same(different_stage_result, expected_diff_stage_result)
