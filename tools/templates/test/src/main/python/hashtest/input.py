import smv

class table1(smv.SmvCsvFile):
    def path(self):
        return "hashtest/table.csv"

class table2(smv.SmvCsvFile):
    def path(self):
        return "hashtest/table.csv"
