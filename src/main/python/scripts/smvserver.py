# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import os
import fnmatch
import re
import glob
import json
from flask import Flask, request, jsonify
from smv import SmvApp
from smv.smvapp import DataSetRepoFactory
from shutil import copyfile
import py_compile
import json
from smv.smvdataset import SmvCsvFile
from smv.smvdataset import SmvHiveTable
import ast
import errno

app = Flask(__name__)

TAB_SIZE = 4

# ---------- Helper Functions ---------- #

def getStagesInApp():
    """returns list of all stages defined in app"""
    return list(SmvApp.getInstance().stages)

def getFqnsInApp():
    """returns all known module FQNs in app. Note: excluded links"""
    repo = DataSetRepoFactory(SmvApp.getInstance()).createRepo()
    # generate list of URNs in a stage for each stage (list-of-list)
    urnsLL = [repo.dataSetsForStage(s) for s in getStagesInApp()]
    # flatten the list-of-list to simple list of urns and remove the "mod:" prefix
    urns = [u.split(":")[1] for ul in urnsLL for u in ul]
    return urns

def indentation(tabbed_str):
    '''returns the number of indentation spaces for a given string'''
    no_tabs_str = tabbed_str.expandtabs(TAB_SIZE)
    # if string has only whitespace, return 0 indentation.
    # else return length of string minus len of str with preceding whitespace stripped
    return 0 if no_tabs_str.isspace() else len(no_tabs_str) - len(no_tabs_str.lstrip())

def test_compile_for_errors(fullname):
    '''returns true if a given file has compilation errors. False otherwise'''
    error = None
    try:
        ok = py_compile.compile(fullname, None, None, True)
    except py_compile.PyCompileError,err:
        error = err.msg
    except IOError, e:
        error = e
    else:
        if ok == 0:
            error = 1
    return error

def getPlutoImportsStartEnd(linesOfCodeList, importsSectionName):
    '''retuns start and end lines of a given imports section'''
    blockStart = "###---{}_IMPORTS_START---###".format(importsSectionName)
    blockEnd = "###---{}_IMPORTS_END---###".format(importsSectionName)

    blockStartLine = None
    blockEndLine = None
    for i, line in enumerate(linesOfCodeList):
        if not blockStartLine and line.lstrip().startswith(blockStart):
            blockStartLine = i
            continue
        if blockStartLine is not None and line.lstrip().startswith(blockEnd):
            blockEndLine = i
            break
    return (blockStartLine, blockEndLine)

# TODO: getCodeBlockStartEnd, getDatasetClassStartEnd, getDatasetDescriptionStartEnd should return an Object
# TODO: getCodeBlockStartEnd should use getDatasetClassStartEnd to get class start end and stay dry
def getCodeBlockStartEnd(linesOfCodeList, className, blockName):
    '''returns start and end lines of a given method within a given class'''
    blockStartByName = {
        "imports": "###---GLOBAL_IMPORTS_START---###",  # TODO: Remove. use getPlutoImportsStartEnd
        "run": "def run(",
        "description": "def description(",
        "requiresDS": "def requiresDS(",
        "isEphemeral": "def isEphemeral(",
        "path": "def path(",
        "tableName": "def tableName(",
        "*": "def"
    }
    blockEndByName = { "imports": "###---GLOBAL_IMPORTS_END---###" }

    testModuleClassDef = "class {}(".format(className)
    classDefFound = False
    blockStartLine = None
    blockEndLine = None
    classIndentation = None
    blockIndentation = None
    lastNonEmptyLine = None

    classStart = None
    classEnd = None
    blockStartLine = None
    blockEndLine = None
    numLinesOfCode = len(linesOfCodeList)

    # detect module class def
    for i, line in enumerate(linesOfCodeList):
        if blockName == "imports":  # TODO: remove. use getPlutoImportsStartEnd
            if not blockStartLine and line.lstrip().startswith(blockStartByName[blockName]):
                blockStartLine = i
                continue
            if blockStartLine is not None and line.lstrip().startswith(blockEndByName[blockName]):
                blockEndLine = i
                break
        else:
            # detect class start
            if line.lstrip().startswith(testModuleClassDef):
                classDefFound = True
                classStart = i
                classIndentation = indentation(line)
                continue
            # detect method if not already found
            if (classDefFound and blockStartLine is None) and line.lstrip().startswith(blockStartByName[blockName]):
                blockStartLine = i
                blockIndentation = indentation(line)
                lastNonEmptyLine = i
                continue
            # detect end of method
            if blockStartLine is not None and blockEndLine is None:
                # check if eof or next block started
                if (not line.isspace()) and (indentation(line) <= blockIndentation):
                    blockEndLine = lastNonEmptyLine
                elif i == (numLinesOfCode - 1):
                    # if last line before eof is not empty space, current line is end of method, else last non empty
                    blockEndLine = i if not line.isspace() else lastNonEmptyLine
            # detect class end by start of another class (or block with eq or less indentation)
            if (not line.isspace()) and (classDefFound and (indentation(line) <= classIndentation)):
                classEnd = lastNonEmptyLine
                break;
            # detect class end by eof
            if i == (numLinesOfCode - 1) and classDefFound:
                classEnd = i if not line.isspace() else lastNonEmptyLine
            # mark last line with code. Will be end of method body when next block of code or eof found
            lastNonEmptyLine = i if (not line.isspace()) else lastNonEmptyLine

    return (blockStartLine, blockEndLine, classStart, classEnd)

# TODO: should also return line number of end of class definition
def getDatasetClassStartEnd(lines_of_code_list, module_name):
    '''returns beginning and end lines of a class, and the indentation of its first method'''
    test_module_class_def = "class {}(".format(module_name)
    start = None
    end = None
    methodIndentation = None
    lastNonEmptyLine = None

    # detect module class def
    for i, line in enumerate(lines_of_code_list):
        if start is None and line.lstrip().startswith(test_module_class_def):
            start = i
            continue
        if start is not None:
            # determine indentation of first method # TODO: should check tabsize.. FIXME
            methodDef = re.match(r'^([ \t]+)def', line)
            if methodDef and methodIndentation is None:
                methodIndentation = methodDef.group(1)
                continue
            # check if string not blank and has no indentation, or this is the last line
            if (not line.isspace() and indentation(line) == 0):
                end = lastNonEmptyLine
                break
        lastNonEmptyLine = i if (not line.isspace()) else lastNonEmptyLine
    if start is not None and end is None:
        end = len(lines_of_code_list)  # TODO.. should be last non empty line
    return (start, end, methodIndentation)

def getDatasetDescriptionStartEnd(linesOfCodeList, className):
    '''
    returns a datasets description start and end line numbers
    '''
    (start, end, cDefEnd) = (None, None, None) # placeholders for start and end of description
    # detect first method definition and class
    (mStart, _, cStart, cEnd) = getCodeBlockStartEnd(linesOfCodeList, className, "*")

    if cStart and cEnd:
        for i, line in enumerate(linesOfCodeList[cStart: cEnd + 1]):
            if cDefEnd is None:
                # detect end of class definition, exclude comments after it
                if line.startswith('#'): continue
                lineSplitAtComment = line.split("#")
                nonCommentStr = lineSplitAtComment[0]
                if nonCommentStr.rstrip().endswith(":"):
                    cDefEnd = cStart + i
                    continue
            lstrippedLine = line.lstrip()
            if not start and (lstrippedLine.startswith("\"\"\"") or lstrippedLine.startswith("'''")):
                start = cStart + i

            if start:
                uncommentedLine = line.split("#")[0].rstrip()
                if uncommentedLine.endswith("\"\"\"") or uncommentedLine.endswith("'''"):
                    # if i is start, then """ must have two occurrences for i to be beginning and end
                    if start == (cStart + i) and not (uncommentedLine.count("\"\"\"") == 2 or uncommentedLine.count("'''") == 2):
                        continue
                    end = cStart + i
                    break
            # line is already at first method, so there is no docstring
            if (cStart + i) >= mStart:
                (start, end) = (None, None)
                break
    return (start, end, cDefEnd)


def get_filepath_from_moduleFqn(module_fqn):
    '''given a fqn, returns the fullname of its file'''
    # TODO: do not use hardcoded value... FIXME
    prefix = "/projects/sample_smv_project/src/main/python/"
    # dir1.dir2.file.class => [dir1, dir2, file]
    fqn_dirs_filename_list = module_fqn.split(".")[:-1]
    # concats fqn_dirs_filename_list into string with "/" intermezzo, appends .py, prepends prefix
    filepath = prefix + "/".join(fqn_dirs_filename_list) + ".py"
    return filepath

def get_output_dir():
    '''returns the smv app's output directory'''
    output_dir = SmvApp.getInstance().outputDir()
    if (output_dir.startswith('file://')):
        output_dir = output_dir[7:]
    return output_dir

def get_latest_file_dir(output_dir, module_name, suffix):
    '''
    There could be multiple output snapshots for a module in the output dir, like:
        com.mycompany.myproj.stage1.employment.PythonEmploymentByState_13e82a9b.csv
        com.mycompany.myproj.stage1.employment.PythonEmploymentByState_51d96a9b.csv
        ...
    This function is to find the latest output version for certain module.
    '''
    latest_file_dir = max([f for f in os.listdir(output_dir) \
        if f.startswith(module_name) and f.endswith(suffix)], \
        key=lambda f: os.path.getctime(os.path.join(output_dir, f)))
    return os.path.join(output_dir, latest_file_dir)

def read_file_dir(file_dir, limit=999999999):
    '''
    "file_dir" is the data/schema path of SMV output. The SMV output is split into
    multiple parts and stored under "file_dir" like part-00001, part-00002...
    This function is to read lines of all the files and return them as a list.
    User can use the "limit" parameter to return the first n lines.
    '''
    lines = []
    for file in glob.glob('%s/part-*' % file_dir):
        with open(file, 'rb') as readfile:
            for line in readfile.readlines():
                lines.append(line.rstrip())
                if len(lines) >= limit:
                    break
        if len(lines) >= limit:
            break
    return lines

def get_module_code_file_mapping():
    '''
    This function returns a dictionary where the key is the module name and the
    value is the absolute file path of this module.
    '''
    def get_all_files_with_suffix(path, suffix):
        '''
        This function recurseively searches for all the files with certain
        suffix under certain path and return the absolute file names in a list.
        '''
        matches = []
        for root, dirnames, filenames in os.walk(path):
            for filename in fnmatch.filter(filenames, '*.%s' % suffix):
                matches.append(os.path.join(root, filename))
        return matches

    def get_module_file_mapping(files, patterns):
        module_dict = {}
        # compile the patterns
        patterns = [re.compile(pattern) for pattern in patterns]
        for file in files:
            with open(file, 'rb') as readfile:
                for line in readfile.readlines():
                    # in Python 3, readlines() return a bytes-like object
                    if sys.version >= '3': line = line.decode()
                    for pattern in patterns:
                        m = pattern.search(line)
                        if m:
                            module_name = m.group(1).strip()
                            file_name = file
                            fqn = get_fqn(module_name, file_name)
                            if (fqn):
                                module_dict[fqn] = file_name
        return module_dict

    def get_fqn(module_name, file_name):
        '''
        "module_name" is like "EmploymentByState".
        "file_name" is the absolute file path containing this module, like
            "/xxx/xxx/MyApp/src/main/scala/com/mycompany/myproj/stage1/EmploymentByState.scala".
        This function will return the fqn of the module, like
            "com.mycompany.myproj.stage1.EmploymentByState".
        It will work for both scala and python modules.
        '''
        sep = os.path.sep
        patterns = [
            '(.+?)%s(.+?)$' % sep.join(['src', 'main', 'scala', '']),
            '(.+?)%s(.+?)$' % sep.join(['src', 'main', 'python', '']),
        ]
        for pattern in patterns:
            m = re.search(pattern, file_name)
            if m:
                fqn_split = m.group(2).strip().split(sep)
                if fqn_split[-1].endswith('.scala'):
                    fqn_split.pop()
                elif fqn_split[-1].endswith('.py'):
                    fqn_split[-1] = fqn_split[-1][:-3]
                fqn_split.append(module_name)
                fqn = '.'.join(fqn_split)
                return fqn

    code_dir = os.getcwd() + '/src'
    scala_files = get_all_files_with_suffix(code_dir, 'scala')
    python_files = get_all_files_with_suffix(code_dir, 'py')

    files = scala_files + python_files
    patterns = [
        'object (.+?) extends( )+SmvModule\(',
        'object (.+?) extends( )+SmvCsvFile\(',
        'class (.+?)\(SmvModule',
        'class (.+?)\(SmvCsvFile',
    ]
    module_dict = get_module_file_mapping(files, patterns)
    return module_dict

def err_res(err, err_msg="", res={}): # err, errmsg="", res={}
    '''returns a json object with the default format for error responses'''
    retval = {}
    retval["err"] = err
    retval["err_msg"] = err_msg
    retval["res"] = res
    return jsonify(retval)

def ok_res(res):
    '''returns a json object with the default format for a successful response'''
    retval = {}
    retval["err"] = OK
    retval["res"] = res
    return jsonify(retval)

def get_module_name_from_fqn(fqn):
    '''Returns a string contaiting the basename for a given fqn'''
    return fqn.split(".")[-1:][0]   # a.b.c => [a,b,c] => [c] => c

def getMsnFromFqn(fqn):
    '''based on a fqn, returns an object with the basename, and stage for that given fqn'''
    fqn_split = fqn.split(".")
    stage = getStageFromFqn(fqn)
    baseName = fqn_split[-1]
    return { "stage": stage, "baseName": baseName }

def getStageFromFqn(fqn):
    '''returns the stage given a a dataset's fqn'''
    # constructing urn for dataset
    try:
        stage = SmvApp.getInstance().getStageFromFqn(fqn).encode("utf-8")
    except:
        raise ValueError("Could not retrive stage with the given fqn: " + str(fqn))
    return stage

# TODO: FIXME: test if works with new smv version
def getDatasetInfo(fqn, baseName, stage):
    '''returns an object containing the properties of a dataset. Namely, fqn, name, stage, isEphemeral, dsType, description,
        requiresDS (modules), tableName (Hive), csvFile (csv)'''
    try:
        module = DataSetRepoFactory(SmvApp.getInstance()).createRepo().loadDataSet(fqn)
    except:
        return None # module does noe exist

    moduleDsType = module.dsType();
    res = {
        "fqn": module.fqn(),
        "moduleName": baseName,
        "stage": stage,
        "ephemeral": module.isEphemeral(),
        "dsType": moduleDsType,
        "description": module.description()
    }

    if moduleDsType.lower() == "input":
        if isinstance(module, SmvCsvFile):
            res["dsInputType"] = 'csv'
            res["inputFile"] = module.path()
        if isinstance(module, SmvHiveTable):
            res["dsInputType"] = 'hive'
            res["tableName"] = module.tableName()
        if not res["dsInputType"]: raise ValueError('dsInputType not supported')
    elif moduleDsType.lower() == "module":
        res["requiresDS"] = map(lambda ds: ds.fqn(), module.requiresDS())
    else:
        raise ValueError("dsType not supported")

    file_name = get_filepath_from_moduleFqn(fqn)
    module_name = get_module_name_from_fqn(fqn)

    # try:
    with open(file_name, 'rb') as f:
        lines_of_code_list = f.readlines()
    file_content = [line.rstrip() for line in lines_of_code_list]
    (run_start, run_end, _, _) = getCodeBlockStartEnd(lines_of_code_list, module_name, 'run')

    # remove indentation from run method body
    raw_run_method_body = file_content[(run_start + 1):(run_end + 1)]
    run_body_indent = indentation(raw_run_method_body[0])
    run_method_body = [line[run_body_indent:] for line in raw_run_method_body]

    res["srcFile"] = file_name,
    res["srcCode"] = run_method_body
    return res


def extractImportsFromFqns(fqns = []):
    '''
    Given a list of fqns, returns a list of file qualified names. E.g.:
    for: fqns = ['a.b.c.d', 'x.y.z'], returns: ['a.b.c', 'x.y'], which are files located in a/b/c.py, and x/y.py
    '''
    return list(map(lambda fqn: ".".join(fqn.split(".")[:-1]), fqns))

def buildImports(className = None, requiresDS = None, requiredDSLinks = []):
    '''Template for generating the imports section of a dataset'''
    # classname not provided, then create global imports
    if not className:
        return """\
###---GLOBAL_IMPORTS_START---###
from smv import *
from pyspark.sql.functions import *
###---GLOBAL_IMPORTS_END---###\n"""

    # class has no required datasets, no need to import anything
    if not requiresDS: return "";

    dependencies = extractImportsFromFqns(requiresDS)
    importStatements = "\n".join(map(lambda dep: "import {}".format(dep), dependencies))
    linkDefinitions = "\n".join(["{0} = SmvModuleLink({1})".format(l.varName, l.fqn) for l in requiredDSLinks])
    return """\
###---{0}_IMPORTS_START---###
{1}

{2}
###---{0}_IMPORTS_END---###\n""".format(className, importStatements, linkDefinitions)

# class start
def buildClassStart(className, dsType): # what about SmvOutput?
    '''Template for generating the class definition of a dataset'''
    extendsByDsType = { "csv": "SmvCsvFile", "hive": "SmvHiveTable", "module": "SmvModule" }
    extends = extendsByDsType[dsType.lower()]
    return "class {}({}):\n".format(className, extends)

# build description
def buildDescription(description, indentation="\t"):
    '''Template for generating the description method\'s code'''
    return """\
{1}\"\"\"
{1}{0}
{1}\"\"\"\n""".format(description, indentation)

def buildRun(dsType, body=None, indentation="\t"):
    '''Template for generating the run method code'''
    if body is None:
        newBody = "{0}{0}return None\n".format(indentation)
    else: # give 2 level indentation to body
        bodyList = body.splitlines(True)
        newBody = "".join(["{0}{0}{1}".format(indentation, line) for line in bodyList])
    arg = "i" if dsType.lower() == "module" else "df"
    return "{2}def run(self, {0}):\n{1}".format(arg, newBody, indentation)

def buildIsEphemeral(ephemeral, indentation="\t"):
    '''Template for generating isEphemeral function code'''
    return "\
{1}def isEphemeral(self):\n\
{1}{1}return {0}\n".format(ephemeral, indentation)

# module
def buildRequiresDS(requiresDS, indentation="\t"):
    '''Template for generating requiresDS\' method code'''
    requires = "" if not requiresDS else ", ".join(map((lambda r: r.encode("utf-8")), requiresDS))
    return "\
{1}def requiresDS(self):\n\
{1}{1}return [{0}]\n".format(requires, indentation)

def getRequiredModulesWithLinks(currentFqn, requiredFqns):
    '''takes as arguments a fqn (currentFqn) and a list of required fqns.
        Returns a list as it should be consumed by the requiresDS method of a dataset.
        The returned list contains:
        - fqn: for every requiredFqn that is in the same stage as currentFqn
        - SmvModuleLink variable name: for every requiredFqn in a different stage than currentFqn
         E.g:
        currentFqn = 'stage1.foo.foo'
        requiredFqns= [stage1.bar.bar, stage2.baz.baz]
        Will return: [
            stage1.foo.foo, # it is in same stage as current fqn, so returns fqn
            stage_2_baz_baz_link  # different stage.. returns variable name
        ] '''
    requiresDSList = []
    currentStage = getStageFromFqn(currentFqn)

    for fqn in requiredFqns:
        # if same stage, fqn
        if currentStage == getStageFromFqn(fqn):
            requiresDSList.append(fqn)
        # if different stage, wrap around link
        else:
            requiresDSList.append(RequiredDSLink(fqn).varName)
    return requiresDSList

def getRequiredDSLinks(currentFqn, requiredFqns):
    '''takes as arguments a fqn (current fqn) and a list of requiredFqns.
        Returns a list of RequiredDSLink objects for each fqn that is in a different started
        than the currentFqn. Each object contains a fqn mapped to SmvModuleLink variable name.

        E.g.:
        currentFqn = 'stage1.foo.foo'
        requiredFqns = [ stage1.bar.bar, stage2.baz.baz ]
            returns [ (a RequiredDSLink object for stage2.baz.baz) ] '''

    linkVarsList = []
    currentStage = getStageFromFqn(currentFqn)
    for fqn in requiredFqns:
        if currentStage != getStageFromFqn(fqn):
            linkVarsList.append(RequiredDSLink(fqn))
    return linkVarsList

# csv
def buildPath(path, indentation="\t"):
    '''Template for generating path method code'''
    path = "" if path is None else path
    return "\
{1}def path(self):\n\
{1}{1}return \"{0}\"\n".format(path, indentation)

# hive
def buildTableName(tableName, indentation="\t"):
    '''Template for generating tableName method code'''
    tableName = "" if tableName is None else tableName
    return "\
{1}def tableName(self):\n\
{1}{1}return \"{0}\"\n".format(tableName, indentation)


def generateModuleCode(stages, className, dsType, description, isEphemeral, run=None, requiresDS=None):
    '''Basic template for generating datasets extending SmvModule'''
    return "{}\n{}{}{}\n{}\n{}\n".format(
        buildImports(),  # should be list of stages, not hardcoded
        buildClassStart(className, dsType),
        buildDescription(description),
        buildRequiresDS(requiresDS),
        buildIsEphemeral(isEphemeral),
        buildRun(dsType, run)).expandtabs(TAB_SIZE) # ...and turn tabs to spaces

def generateCsvCode(stages, className, dsType, description, isEphemeral, run=None, path=None):
    '''Basic template for generating datasets extending SmvCsvFile'''
    return "{}\n{}{}{}\n{}\n{}\n".format(
        buildImports(),  # should be list of stages, not hardcoded
        buildClassStart(className, dsType),
        buildDescription(description),
        buildPath(path),
        buildIsEphemeral(isEphemeral),
        buildRun(dsType, run)).expandtabs(TAB_SIZE) # ...and turn tabs to spaces

def generateHiveCode(stages, className, dsType, description, isEphemeral, run=None, tableName=None):
    '''Basic template for generating datasets extending SmvHiveTable'''
    return "{}\n{}{}{}\n{}\n{}\n".format(
        buildImports(),  # should be list of stages, not hardcoded
        buildClassStart(className, dsType),
        buildDescription(description),
        buildTableName(tableName),
        buildIsEphemeral(isEphemeral),
        buildRun(dsType, run)).expandtabs(TAB_SIZE) # ...and turn tabs to spaces


# ---------- API Definition ---------- #

MODULE_NOT_PROVIDED_ERR = 'ERROR: No module name provided!'
MODULE_NOT_FOUND_ERR = 'ERROR: Job failed to run. Please check whether the module name is valid!'
MODULE_ALREADY_EXISTS_ERR = 'ERROR: Module already exists!'
TYPE_NOT_PROVIDED_ERR = 'ERROR: No module type provided!'
TYPE_NOT_SUPPORTED_ERR = 'ERROR: Module type not supported!'
CODE_NOT_PROVIDED_ERR = 'ERROR: No module code provided!'
COMPILATION_ERROR = 'ERROR: Compilation error'
JOB_SUCCESS = 'SUCCESS: Code updated!' # TODO: rename CODE_UPDATE_SUCCESS
OK = ""

MODULE_DOES_NOT_EXIST= 'MODULE_DOES_NOT_EXIST'

@app.route("/api/get_app_info", methods = ['POST'])
def get_app_info():
    '''
    body: empty
    function: retrieve list of stages and fqns in app.
    '''
    res = {
        "stages": getStagesInApp(),
        "fqns": getFqnsInApp()
    }
    return ok_res(res)

@app.route("/api/run_module", methods = ['POST'])
def run_module():
    '''
    body: fqn = 'xxx' (fqn)
    function: run the module
    '''
    try:
        module_fqn = request.form['fqn'].encode("utf-8")
    except:
        raise err_res('MODULE_NOT_PROVIDED_ERR')

    run_result = SmvApp.getInstance().runModule("mod:{}".format(module_fqn))
    return ok_res(str(run_result))

# TODO: move
def getFqnOfRequire(ds):
    '''returns fqn of a dataset. If ds is a link, will return fqn of target'''
    if getattr(ds, 'IsSmvModuleLink', None):
        ds = ds.target()
    return ds.fqn()

# TODO: rename... should return all information about the module or create if not exists
@app.route("/api/get_dataset_info", methods = ['POST'])
def get_module_code():
    '''
    body: fqn = 'xxx'
    function: return the module's code
    '''

    module_fqn = None
    module_name = None
    module_stage = None
    try:
        module_name = request.form['moduleName'].encode("utf-8")
        module_stage = None
        if "." in module_name: # module_name is an fqn
            module_fqn = module_name
            msn = getMsnFromFqn(module_fqn)
            module_stage = msn["stage"]
            module_name = msn["baseName"]
        else: # if module_name is not an fqn, both name and stage must not be empty
            module_stage = request.form['stage'].encode("utf-8")
            if not module_name or not module_stage:
                raise ValueError('TODO....')
            module_fqn = "{0}.{1}.{1}".format(module_stage, module_name)
    except:
        return err_res('module name and stage not provided')

    try:
        module = DataSetRepoFactory(SmvApp.getInstance()).createRepo().loadDataSet(module_fqn)
    except:
        return err_res(MODULE_DOES_NOT_EXIST, "Module does not exist yet", {
            "moduleName": module_name,
            "stage": module_stage,
            "fqn": module_fqn,
        })

    moduleDsType = module.dsType();
    res = {
        "fqn": module.fqn(),
        "moduleName": module_name,
        "stage": module_stage,
        "ephemeral": module.isEphemeral(),
        "dsType": moduleDsType,
        "description": module.description().strip() if module.description() else ""
    }

    if moduleDsType.lower() == "input":
        if isinstance(module, SmvCsvFile):
            res["dsInputType"] = 'csv'
            res["inputFile"] = module.path()
        if isinstance(module, SmvHiveTable):
            res["dsInputType"] = 'hive'
            res["tableName"] = module.tableName()
        if not res["dsInputType"]: raise ValueError('dsInputType not supported')
    elif moduleDsType.lower() == "module":
        res["requiresDS"] = map(lambda ds: getFqnOfRequire(ds), module.requiresDS())
    else:
        raise ValueError("dsType not supported")

    file_name = get_filepath_from_moduleFqn(module_fqn)
    module_name = get_module_name_from_fqn(module_fqn)

    # try:
    with open(file_name, 'rb') as f:
        lines_of_code_list = f.readlines()
    file_content = [line.rstrip() for line in lines_of_code_list]
    (run_start, run_end, _, _) = getCodeBlockStartEnd(lines_of_code_list, module_name, 'run')

    run_method_body = [""]
    if run_start and run_end:
        # remove indentation from run method body
        raw_run_method_body = file_content[(run_start + 1):(run_end + 1)]
        run_body_indent = indentation(raw_run_method_body[0])
        run_method_body = [line[run_body_indent:] for line in raw_run_method_body]
    else:
        err_res('run method not detected in dataset')

    res["srcFile"] = file_name,
    res["srcCode"] = run_method_body
    return ok_res(res)
    # except IOError:
    #     return err_res(MODULE_NOT_FOUND_ERR)




@app.route("/api/create_dataset", methods = ['POST']) # rename create_dataset
def updateModuleMetaData():
    '''
        fqn
        description
        dsType
        ephemeral
    '''
    fqn = request.form["fqn"].encode("utf-8")
    msn = getMsnFromFqn(fqn)
    className = msn["baseName"]
    description = request.form["description"].encode("utf-8")
    dsType = request.form["dsType"].encode("utf-8").lower()
    isEphemeral = ast.literal_eval(request.form["ephemeral"].encode("utf-8").capitalize())

    # ----TODO: use getDatasetInfo() for dsProperties when working!!!!!!----
    dsProperties = { "fqn":fqn, "description":description, "dsType":dsType, "srcCode":["return None\n"], "ephemeral":isEphemeral }

    if dsType == 'input':
        dsInputType = request.form["dsInputType"].encode("utf-8").lower()

        if dsType == 'input':
            if dsInputType == 'csv': dsProperties["fileName"] = ""
            if dsInputType == 'hive': dsProperties["tableName"] = ""
        # ---------------------------------------------------------------------
        # TODO: remove line below when dsTye in SmvCsvFile and SmvHiveTable overriden
        dsType = dsInputType

    try:
        module = DataSetRepoFactory(SmvApp.getInstance()).createRepo().loadDataSet(fqn)
    except ImportError:
        # dataset does not exist.. create new module
        stagesList = ['airlineapp.etl', 'airlineapp.datamodel', 'airlineapp.feature']  # TODO: get actual list of stages.. not hardcoded

        newDsSrcCode = ''
        if dsType == 'module':
            newDsSrcCode = generateModuleCode(stagesList, className, dsType, description, isEphemeral)
        elif dsType == 'csv':
            newDsSrcCode = generateCsvCode(stagesList, className, dsType, description, isEphemeral)
        elif dsType == 'hive':
            newDsSrcCode = generateHiveCode(stagesList, className, dsType, description, isEphemeral)
        else:
            raise ValueError('dsType {} not supported'.format(dsType))

        # full name of file to be created
        newFile = get_filepath_from_moduleFqn(fqn)
        # create dir if not exists
        if not os.path.exists(os.path.dirname(newFile)):
            try:
                os.makedirs(os.path.dirname(newFile))
            except OSError as exc:
                    if exc.errno != errno.EEXIST:
                        raise
        # create file
        with open(newFile, 'w') as fd:
            fd.write(newDsSrcCode)

        # compile duplicate
        compileHasErrors = test_compile_for_errors(newFile)
        # remove .pyc created by compilation
        if os.path.isfile(newFile + 'c'):
            os.remove(newFile + 'c')

        if compileHasErrors:
            os.remove(newFile)  # remove file with errors
            return err_res(COMPILATION_ERROR, "Module failed to compile", compileHasErrors)

        # TODO: FIXME: fix and use getDatasetInfo function
        #dsProperties = getDatasetInfo(fqn, msn["baseName"], msn["stage"])
        # TODO: check if dsProperties is None... raise exception... could not get code from created ds
        dsProperties["srcFile"] = newFile,

        return ok_res({ "msg":"Dataset created", "dsProperties":dsProperties})

    # TODO: if module exists, return its data
    return err_res("Module already exists")

@app.route("/api/update_dataset_info", methods = ['POST'])
def updateDatasetInfo():
    '''
        fqn
        description
        dsType
        srcCode
        ephemeral

        requiresDS

        dsInputType
        tableName
        fileName
    '''
    fqn = request.form["fqn"].encode("utf-8")
    msn = getMsnFromFqn(fqn)
    className = msn["baseName"]
    description = request.form["description"].encode("utf-8")
    dsType = request.form["dsType"].encode("utf-8").lower()
    srcCode =  None if not request.form['srcCode'] else "".join(map((lambda c: c.encode("utf-8")), json.loads(request.form["srcCode"])))
    isEphemeral = ast.literal_eval(request.form["ephemeral"].encode("utf-8").capitalize())
    requiresDS = None
    dsInputType = None
    inputFile = None
    fileName = None
    newFileContents = None

    if dsType == 'module':
        # requiresDS = request.form["requiresDS"]
        try:
            requiresDS = ast.literal_eval(request.form["requiresDS"])
        except:   # TODO: use exception name instead of catch all
            pass
    if dsType == 'input':
        dsInputType = request.form["dsInputType"].encode("utf-8").lower()
        # TODO: inputFile, filename should be renamed to path
        if dsInputType == 'csv': fileName = request.form["inputFile"].encode("utf-8") # TODO.. use same keys in req
        if dsInputType == 'hive': tableName = request.form["tableName"].encode("utf-8")
        dsType = dsInputType

    dsProperties = { "fqn":fqn, "description":description, "dsType":request.form["dsType"], "srcCode":srcCode, "ephemeral":isEphemeral}
    if dsType == 'input':
        if dsInputType == 'csv':
            dsProperties["fileName"] = fileName
        if dsInputType == 'hive': test["tableName"] = tableName
    if dsType == 'module':
        dsProperties["requiresDS"] = requiresDS

    try:
        module = DataSetRepoFactory(SmvApp.getInstance()).createRepo().loadDataSet(fqn)
    except ImportError:
        return 'does not exist'


    fileName = get_filepath_from_moduleFqn(fqn) # TODO: handle dne
    # if file exists.. create copy
    if os.path.isfile(fileName):
        duplicateFileName = fileName[:-3] + "_smv_update_code_duplicate.py" # -3 is len(".py")
        copyfile(fileName, duplicateFileName)

        lines_of_code = None                    # counter for number of lines of code
        lines_of_code_list = [];                # list containing a string for each line of code

        # open duplicate and update its run method with code input
        with open(duplicateFileName, 'r+') as fd:
            lines_of_code_list = fd.readlines()
            lines_of_code = len(lines_of_code_list)

            # placeholder for the updated code
            newFileContents = lines_of_code_list
            # get indentation for class methods
            (_, _, methodIndent) = getDatasetClassStartEnd(lines_of_code_list, className)

            ## description: get start, end of class definition
            (descStart, descEnd, cDefEnd) = getDatasetDescriptionStartEnd(newFileContents, className);
            descriptionSection = buildDescription(description, methodIndent).splitlines(True)

            newFileContents = newFileContents[:cDefEnd + 1] + descriptionSection + newFileContents[cDefEnd + 1:] \
                if descStart is None else newFileContents[:descStart] + descriptionSection + newFileContents[descEnd + 1:]

            ## ephemeral
            (ephStart, ephEnd, _, ephClassEnd) = getCodeBlockStartEnd(newFileContents, className, "isEphemeral");
            ephSection = buildIsEphemeral(isEphemeral, methodIndent).splitlines(True)
            newFileContents = newFileContents[:ephClassEnd + 1] + ephSection + newFileContents[ephClassEnd + 1:] \
                if ephStart is None else newFileContents[:ephStart] + ephSection + newFileContents[ephEnd + 1:]

            ## run
            (rStart, rEnd, _, rClassEnd) = getCodeBlockStartEnd(newFileContents, className, "run");
            runSection = buildRun(dsType, srcCode, methodIndent).splitlines(True)  # TODO: rename srcCode run
            newFileContents = newFileContents[:rClassEnd + 1] + ["\n"] + runSection + newFileContents[rClassEnd + 1:] \
                if rStart is None else newFileContents[:rStart] + runSection + newFileContents[rEnd + 1:]

            if dsType.lower() == 'module':
                # detect class imports start, end
                (classImportsStart, classImportsEnd) = getPlutoImportsStartEnd(lines_of_code_list, className)
                # process requiresDS, turn them into links, get link definitions
                reqDSWLinks = getRequiredModulesWithLinks(fqn, requiresDS)
                reqDSLinkVars = getRequiredDSLinks(fqn, requiresDS)
                classImportsSection = buildImports(className, requiresDS, reqDSLinkVars).splitlines(True)
                if classImportsStart:
                    newFileContents = classImportsSection + newFileContents[:classImportsStart] + newFileContents[classImportsEnd + 1:]
                else:
                    newFileContents = classImportsSection + newFileContents

                (reqStart, reqEnd, _, reqClassEnd) = getCodeBlockStartEnd(newFileContents, className, "requiresDS");
                reqSection = buildRequiresDS(reqDSWLinks, methodIndent).splitlines(True)
                newFileContents = newFileContents[:reqClassEnd + 1] + ["\n"] + reqSection + newFileContents[reqClassEnd + 1:] \
                    if reqStart is None else newFileContents[:reqStart] + reqSection + newFileContents[reqEnd + 1:]

            # else if csv, path
            if dsType.lower() == 'csv':
                (pthStart, pthEnd, _, pthClassEnd) = getCodeBlockStartEnd(newFileContents, className, "path");
                pthSection = buildPath(fileName, methodIndent).splitlines(True) # TODO rename fileName path
                newFileContents = (
                    newFileContents[:pthClassEnd + 1] + ["\n"] + pthSection + newFileContents[pthClassEnd + 1:]
                    if pthStart is None else newFileContents[:pthStart] + pthSection + newFileContents[pthEnd + 1:])
            # else if hive, tableName
            elif dsType.lower() == 'hive':
                 (tabStart, tabEnd, _, tabClassEnd) = getCodeBlockStartEnd(newFileContents, className, "tableName");
                 tabSection = buildTableName(tableName, methodIndent).splitlines(True)
                 newFileContents = (
                     newFileContents[:tabClassEnd + 1] + ["\n"] + tabSection + newFileContents[tabClassEnd + 1:]
                     if tabStart is None else newFileContents[:tabStart] + tabSection + newFileContents[tabEnd + 1:])

            # detect global imports
            (importsStart, importsEnd, _, _) = getCodeBlockStartEnd(newFileContents, None, "imports")
            importsSection = buildImports(None).splitlines(True) # TODO... should use stages list as argument
            newFileContents = (importsSection + newFileContents if importsStart is None
                else importsSection + newFileContents[:importsStart] + newFileContents[importsEnd + 1:])

            # modify duplicate
            fd.seek(0)  # reset file stream
            fd.truncate()
            # will write new code to file
            for i in xrange(len(newFileContents)):
                fd.write(newFileContents[i])
        # compile duplicate
        compileHasErrors = test_compile_for_errors(duplicateFileName)
        # remove duplicate and its .pyc
        os.remove(duplicateFileName)
        if os.path.isfile(duplicateFileName + 'c'):
            os.remove(duplicateFileName + 'c')
        if compileHasErrors:
            return err_res(COMPILATION_ERROR, "Module failed to compile", compileHasErrors)
        # if duplicate's compile successfull, modify the code of the original file
        with open(fileName, 'w') as fd:
            # for i in xrange(len(updated_code)):
            #     fd.write(updated_code[i])
            for i in xrange(len(newFileContents)):
                fd.write(newFileContents[i])
        return ok_res(JOB_SUCCESS)
    else:
        return err_res(MODULE_NOT_FOUND_ERR)

@app.route("/api/get_sample_output", methods = ['POST'])
def get_sample_output():
    '''
    body: fqn = 'xxx' (fqn)
    function: return the module's sample output
    '''
    try:
        module_fqn = request.form['fqn'].encode("utf-8")
    except:
        raise err_res(MODULE_NOT_PROVIDED_ERR)

    # run and get DataFrame
    module_fqn = request.form['fqn'].encode("utf-8")
    df = SmvApp.getInstance().runModule("mod:{}".format(module_fqn))
    # get first 10 entries from dataframe
    raw_sample_output = df.limit(10).collect()
    # express each row as a dict
    sample_output_as_dict = list(map((lambda row: row.asDict()), raw_sample_output))
    df_fields = list(map((lambda field: field.jsonValue()), df.schema.fields))

    retval = { "schema": df_fields, "rows": sample_output_as_dict, "fqn": module_fqn }
    return ok_res(retval)

@app.route("/api/get_module_schema", methods = ['POST'])
def get_module_schema():
    '''
    body: name = 'xxx' (fqn)
    function: return the module's schema
    '''
    try:
        module_name = request.form['name']
    except:
        raise ValueError(MODULE_NOT_PROVIDED_ERR)

    try:
        output_dir = get_output_dir()
        latest_dir = get_latest_file_dir(output_dir, module_name, '.schema')
        res = read_file_dir(latest_dir)
        return jsonify(res=res)
    except:
        raise ValueError(MODULE_NOT_FOUND_ERR)

@app.route("/api/get_graph_json", methods = ['POST'])
def get_graph_json():
    '''
    body: none
    function: return the json file of the entire dependency graph
    '''
    res = SmvApp.getInstance().get_graph_json()
    return jsonify(graph=res)

@app.route("/api/get_dataset_src", methods = ['POST'])
def getDatasetSrc():
    '''
    fqn
    '''
    # get form values
    fqn = request.form["fqn"].encode("utf-8").strip()
    # get filename from fqn
    fileName = get_filepath_from_moduleFqn(fqn)

    try:
        with open(fileName, 'r') as f:
            linesOfCodeList = f.readlines()
    except IOError:
        return err_res("Failed to get src code")

    fileContents = [line.rstrip() for line in linesOfCodeList]
    res = { "fullSrc": fileContents }
    return ok_res(res)

@app.route("/api/update_dataset_src", methods = ['POST'])
def updateDatasetSrc():
    '''
    fqn
    '''

    # get form values
    fqn = request.form["fqn"].encode("utf-8").strip()
    fullSrc = "".join(map((lambda c: c.encode("utf-8")), json.loads(request.form["fullSrc"])))

    # get filename from fqn
    fileName = get_filepath_from_moduleFqn(fqn)
    duplicateFileName = fileName[:-3] + "_smv_update_code_duplicate.py"

    # check if filename exists, else return error
    if not os.path.isfile(fileName):
        return err_res(MODULE_NOT_FOUND_ERR, "Source file for {} not found".format(fqn))

    # create duplicate to write and compile
    with open(duplicateFileName, 'w') as fd:
        for i in xrange(len(fullSrc)):
            fd.write(fullSrc[i])

        # compile duplicate
        compileHasErrors = test_compile_for_errors(duplicateFileName)
        if os.path.isfile(duplicateFileName): os.remove(duplicateFileName)
        if os.path.isfile(duplicateFileName + 'c'):
            os.remove("{}c".format(duplicateFileName))

        if compileHasErrors:
            return err_res(COMPILATION_ERROR, "Dataset failed to compile", compileHasErrors)

    with open(fileName, 'w') as fd:
        for i in xrange(len(fullSrc)):
            fd.write(fullSrc[i])

    return ok_res({ "msg": "Dataset updated", "fullSrc": fullSrc })

# Wrapper so that other python scripts can import and then call
# smvserver.Main()
class Main(object):
    def __init__(self):
        options = self.parseArgs()

        # init Smv context
        smvApp = SmvApp.createInstance([])

        # to reduce complexity in SmvApp, keep the rest server single-threaded
        app.run(host=options.ip, port=int(options.port), threaded=False, processes=1)

    def parseArgs(self):
        from optparse import OptionParser
        parser = OptionParser()
        parser.add_option("--port", dest="port", type="int", default=5000,
                  help="smv-server port number [default=5000]")
        parser.add_option("--ip", dest="ip", type="string", default="0.0.0.0",
                  help="smv-server ip to bind to [default=0.0.0.0]")

        (options, args) = parser.parse_args()
        return options

class RequiredDSLink:
    '''Instantiates a link object that contains a link variable name, and a fqn
        The fqn is the fqn of a given dataset. The Link variable name is the string to be used as a
        variable name when turning a dataset with the given fqn into a SmvModuleLink'''
    def __init__(self, fqn):
        self.fqn = fqn
        self.varName = "{}_link".format(fqn.replace(".", "_"))

# temporary till module source control is implemented
module_file_map = {}

if __name__ == "__main__":
    Main()
