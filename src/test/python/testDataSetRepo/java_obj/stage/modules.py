import smv

java_obj = smv.smvapp.SmvApp.getInstance().j_smvApp

class WhateverModule(smv.SmvModule):
    def run(self, i): return None
    def requiresDS(self): return None
