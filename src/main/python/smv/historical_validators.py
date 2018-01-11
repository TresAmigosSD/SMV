def SmvHistoricalValidators(*validators):
    """Decorator to specify set of validators to apply to an SmvDataSet.

        Each validator should specify the standard `metadata` and `validateMetadata`
        methods.  In addition, each validator will have a unique key defined that
        will determine how it is stored in the final metadata structure.  This
        decorator will take care of composing the union of all metadata from validators
        and decomposing the full metatdata structure into individual pieces for each
        validator (hint: the _key() method of each validator is used to key into the
        fine metadata structure)
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
            v_cur_metadata = cur_metadata.get(k)
            v_hist_metadata = [m[k] for m in hist_metadata if m.get(k)]

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

        Derived classes must override the `key`, `metadata`, and `validateMetadata`
        methods.
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


# TODO: move to other repo.

class DistinctCountVariation(smv.SmvHistoricalValidator):
    def __init__(self, col, threshold):
        super(DistinctCountVariation, self).__init__(col, threshold)
        self.col = col
        self.threshold = threshold

    def metadata(self, df):
        count = df.select(self.col).distinct().count()
        return {"d_count": count}

    def validateMetadata(self, cur, hist):
        if len(hist) == 0: return None
        hist_count_avg = float(sum([h["d_count"] for h in hist])) / len(hist)
        cur_count = cur["d_count"]

        #print ("hist_count_avg = " + str(hist_count_avg))
        #print ("cur_count = " + str(cur_count))
        if (float(abs(cur_count - hist_count_avg)) / hist_count_avg) > self.threshold:
            return "DistinctCountVariation: count = %d, avg = %g" % (cur_count, hist_count_avg)
        return None

class AvgValueVariation(smv.SmvHistoricalValidator):
    def __init__(self, col, threshold):
        super(AvgValueVariation, self).__init__(col, threshold)
        self.col = col
        self.threshold = threshold

    def metadata(self, df):
        c_avg = df.agg(F.avg(self.col).alias("a")).collect()[0].a
        return {"avg": c_avg}

    def validateMetadata(self, cur, hist):
        #print("AvgValueVariation.validate: %s ... %s" % (str(cur), str(hist)))
        if len(hist) == 0: return None
        hist_avg_avg = float(sum([h["avg"] for h in hist])) / len(hist)
        cur_avg = cur["avg"]

        #print ("hist_avg_avg = " + str(hist_avg_avg))
        #print ("cur_avg = " + str(cur_avg))
        if (float(abs(cur_avg - hist_avg_avg)) / hist_avg_avg) > self.threshold:
            return "AvgValueVariation: curr avg = %g, hist avg = %g" % (cur_avg, hist_avg_avg)
        return None

class ValueRangeVariation(smv.SmvHistoricalValidator):
    def __init__(self, col, threshold):
        super(ValueRangeVariation, self).__init__(col, threshold)
        self.col = col
        self.threshold = threshold

    def metadata(self, df):
        min_max = df.agg(
            F.min(self.col).alias("min"),
            F.max(self.col).alias("max")).collect()[0]
        return {"min": min_max.min, "max": min_max.max}

    def validateMetadata(self, cur, hist):
        print("ValueRangeVariation.validate: %s ... %s" % (str(cur), str(hist)))
        if len(hist) == 0: return None
        hist_avg_min = float(sum([h["min"] for h in hist])) / len(hist)
        hist_avg_max = float(sum([h["max"] for h in hist])) / len(hist)
        cur_min = cur["min"]
        cur_max = cur["max"]

        print ("hist_avg_min = " + str(hist_avg_min))
        print ("hist_avg_max = " + str(hist_avg_max))
        print ("cur_min = " + str(cur_min))
        print ("cur_max = " + str(cur_max))
        if ((float(abs(cur_min - hist_avg_min)) / hist_avg_min) > self.threshold) or \
           ((float(abs(cur_max - hist_avg_max)) / hist_avg_max) > self.threshold) :
            return "ValueRangeVariation: curr min = %d, hist avg min = %g, cur max = %g, hist avg max = %g" % \
                (cur_min, hist_avg_min, cur_max, hist_avg_max)
        return None
