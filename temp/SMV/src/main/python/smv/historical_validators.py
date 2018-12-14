def SmvHistoricalValidators(*validators):
    """Decorator to specify set of validators to apply to an SmvGenericModule.

        Each validator should specify the standard `metadata` and `validateMetadata`
        methods.  In addition, each validator will have a unique key defined that
        will determine how it is stored in the final metadata structure.  This
        decorator will take care of composing the union of all metadata from validators
        and decomposing the full metadata structure into individual pieces for each
        validator (hint: the _key() method of each validator is used to key into the
        final metadata structure)
    """
    def metadata(self, df):
        """return the union of all `metedata` results from all validators."""
        all_metadata = {}

        for v in validators:
            m = v.metadata(df)
            k = v._key()
            all_metadata[k] = m

        return all_metadata

    def validateMetadata(self, cur_metadata, hist_metadata):
        """validate the results for all validators.  Fail if any validator failed."""
        failed_list = []

        for v in validators:
            # extract the current/historical metadata for this validator
            k = v._key()
            v_cur_metadata = cur_metadata['_userMetadata'].get(k)
            v_hist_metadata = [m['_userMetadata'][k] for m in hist_metadata if m['_userMetadata'].get(k)]

            v_res = v.validateMetadata(v_cur_metadata, v_hist_metadata)
            if v_res != None:
                failed_list.append(v_res)

        if len(failed_list) > 0:
            return ";".join(failed_list)
        return None

    def cls_wrapper(cls):
        cls._smvHistoricalValidatorsList = validators
        cls.metadata = metadata
        cls.validateMetadata = validateMetadata
        return cls

    return cls_wrapper


class SmvHistoricalValidator(object):
    """Base of all user defined historical validator rules.

        Derived classes must override the metadata`, and `validateMetadata` methods.
    """
    def __init__(self, *args):
        self.args = args

    def _key(self):
        """computes a unique "key" for this instance of the validator based on
           the validator arguments and type."""
        key_args = [self.__class__.__name__] + [str(a) for a in self.args]
        return (":".join(key_args))

    def metadata(self, df):
        """identical interface to standard dataset `metadata` method"""
        raise NotImplementedError("missing metadata() method")

    def validateMetadata(self, cur, hist):
        """identical interface to standard dataset `validateMetadata` method"""
        raise NotImplementedError("missing validateMetadata() method")
