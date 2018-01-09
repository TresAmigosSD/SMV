
def metadataForAllValidators(validators, df):
    """Given a seq of validators, return the union of all `metedata` results."""
    all_metadata = {}

    for v in validators:
        m = v.metadata(df)
        k = v.key()
        print("storing metadata at key %s: %s" % (k, str(m)))
        all_metadata[k] = m

    return all_metadata

def SmvHistoricalValidators(*validators):
    """Decorator to specify set of validators to apply to an SmvDataSet
    """
    print("------------------------ inside smv validators: " + str(validators))

    def metadata(self, df):
        return metadataForAllValidators(validators, df)

    def cls_wrapper(cls):
        print("------------------------ inside smv cls_wrapper")
        cls._smvHistoricalValidatorsList = validators
        cls.metadata = metadata
        return cls

    return cls_wrapper

class SmvHistoricalValidator(object):
    """Base of all user defined historical validator rules.

        Derived classes must override the `key`, `metadata`, and `validateMetadata`
        methods.
    """
    def key(self):
        """Derived classes must provide a unique key based on the validator type
            **and** instance arguments.  This is so two rules applied to the
            same dataset will end up with two different metadata keys."""
        raise NotImplementedError("missing key() method")

    def metadata(self, df):
        """identical interface to standard dataset `metadata` method"""
        raise NotImplementedError("missing metadata() method")

    def validateMetadata(self, cur, hist):
        """identical interface to standard dataset `validateMetadata` method"""
        raise NotImplementedError("missing validateMetadata() method")

class DummyRule(SmvHistoricalValidator):
    def __init__(self, col, val):
        self.col = col
        self.val = val

    def key(self):
        return 'dummy:%s:%d' % (self.col, self.val)

    def metadata(self, df):
        return {"c": self.col, "v": self.val}

    def validateMetadata(self, cur, hist):
        print ("===== calling dummy rule validate method")
        return None
