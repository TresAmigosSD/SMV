import sys

from pyspark.sql import SparkSession
from smv import SmvApp

class SmvDriver(object):
    """Driver for an SMV application

        SmvDriver handles the boiler plate around parsing driver args, constructing an SmvApp, and running an application.
        To use SmvDriver, override `main` and in the main block of your driver script call construct your driver and
        call `run`.
    """
    def create_smv_app(self, smv_args, driver_args):
        """Override this to define how this driver's SmvApp is created                

            Default is just SmvApp.createInstance(smv_args). Note that it's important to use `createInstance` to ensure that
            the singleton app is set. 
            
            SmvDriver will parse the full CLI args to distinguish the SMV args from from the args to your driver.
            
            Args:
                smv_args (list(str)): CLI args for SMV - should be passed to `SmvApp`)
                driver_args (list(str)): CLI args for the driver
        """
        sparkSession = SparkSession.builder.\
                enableHiveSupport().\
                getOrCreate() 
        # When SmvDriver is in use, user will call smv-run and interact
        # through command-line, so no need to do py module hotload
        return SmvApp.createInstance(smv_args, sparkSession, py_module_hotload=False)

    def main(self, app, driver_args):
        """Override this to define the driver logic 

            Default is to just call `run` onthe `SmvApp`. 

            Args:
                app (SmvApp): app which was constructed
                driver_args (list(str)): CLI args for the driver
        """
        app.run()

    def run(self):
        """Run the driver
        """
        args = sys.argv[1:]

        try:
            smv_args_end = args.index("--script")
        except ValueError:
            smv_args_end = len(args)

        smv_args = args[:smv_args_end]
        # First arg after smv_args_end is --script
        # Second is the script name
        # Then the driver args start
        driver_args = args[smv_args_end+2:]

        app = self.create_smv_app(smv_args, driver_args)
        self.main(app, driver_args)