# flake8: noqa
# Smv DataSet Framework
from smv.smvmodule import *
from smv.smvinput import *

from smv.smvapp import SmvApp

from smv.runconfig import SmvRunConfig
from smv.csv_attributes import CsvAttributes
from smv.helpers import SmvGroupedData

from smv.historical_validators import SmvHistoricalValidator, SmvHistoricalValidators

# keep old py names for backwards compatibility
SmvPyCsvFile = SmvCsvFile
SmvPyModule = SmvModule
SmvPyOutput = SmvOutput
SmvPyModuleLink = SmvModuleLink

import logging
logger = logging.getLogger(__name__)
stderr_appender = logging.StreamHandler()

# Default logging setting
# These may be overridden when SmvApp initializes
logger.setLevel("INFO")
logger.addHandler(stderr_appender)
