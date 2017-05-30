import abc
import traceback

FUNC_LIST = []

class WithStackTrace(object):
    """abstract class that can be mixed in to allow functions to use the
        'with_stacktrace' decorator.
        This will enable proper error reporting in functions that can be overridden
        by either end-users or developers
    """
    __metaclass__ = abc.ABCMeta

    def __getattribute__(self, name):
        if name in FUNC_LIST:
            obj_getattribute = object.__getattribute__(self, name)

            def catche_errs(*pargs, **kwargs):
                try:
                    res = obj_getattribute(*pargs, **kwargs)
                except BaseException as err:
                    traceback.print_exc()
                    raise err

                return res

            return catche_errs
        else:
            return object.__getattribute__(self, name)

def with_stacktrace(func):
    FUNC_LIST.append(func.__name__)
    return func
