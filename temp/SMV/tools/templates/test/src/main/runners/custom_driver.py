from smv.smvdriver import SmvDriver

class CustomDriver(SmvDriver):
    def main(self, smvApp, driver_args):
        expected_args = ["arg1", "arg2", "arg3"]
        # verify that driver args are parsed correctly
        assert driver_args == expected_args, "Expected driver args {} but got {}".format(expected_args, driver_args)
        # verify that smv args are parsed correctly
        config_key = "smv.config.custom_key"
        config_value = smvApp.py_smvconf.merged_props().get(config_key)
        expected_value = "custom_value"
        assert config_value == expected_value, \
            "Expected config value for {} to be {} but got {}".format(config_key, config_value, expected_value) 
        # verify that smvApp constructed by SmvDriver can run a module
        smvApp.runModuleByName("integration.test.test4.modules.M2")

if __name__ == "__main__":
    CustomDriver().run()