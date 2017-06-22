from smv import *
import traceback

try:
    from integration.test.test4_1.modules import M1

    M1Link = SmvModuleLink(M1)

except:
    traceback.print_exc()
