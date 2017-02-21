from smv import *
import traceback

try:
    from _PROJ_CLASS_.test4_1.modules import M1

    M1Link = SmvPyModuleLink(M1)

except:
    traceback.print_exc()
