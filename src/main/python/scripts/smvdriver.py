import sys

from smv import SmvApp

class SmvDriver(object):
    def create_smv_app(self, smv_args, driver_args):
        return SmvApp.createInstance(smv_args)

    def main(self, app, driver_args):
        app.run()

    def run(self):
        args = sys.argv[1:]

        try:
            smv_args_end = args.index("--script")
        except ValueError:
            smv_args_end = len(args)

        smv_args = args[:smv_args_end]
        driver_args = args[smv_args_end+2:]

        app = self.create_smv_app(smv_args, driver_args)
        self.main(app, driver_args)