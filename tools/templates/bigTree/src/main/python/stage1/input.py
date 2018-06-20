import smv

__all__ = ['EmploymentByState']

class input(smv.SmvCsvFile):
    def path(self):
        return "employment/CB1200CZ11.csv"
