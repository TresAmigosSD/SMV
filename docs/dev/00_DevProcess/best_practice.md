# !!!!Work In Progress (WIP)!!!!

# SMV Development Best Practices

## 1. Task Branch
For each assigned task, a new branch must be created and a Pull Request (PR) created to merge the changes into master.

### Branch Names
* branch names must be of the form `ixxx_desc`, where:
  * `xxx`: the issue number associated with the branch.
  * `desc`: a **very short** description of the branch.  A couple of keywords and abbreviations should suffice.
* Good branch names: `i105_jdbc_connect`, `i705_datasetmgr_mem_leak`
* Bad branch names:
  * `i705_fix_issue_with_datasetmgr_leaking_memory_when_called_twice_after_init` : waaaaay too long of a name.
  * `jdbc_connect` or `jdbc_connect_#105` : does not follow the format above.

### Commits
Commit messages must be of the form "#xxx: description" where `xxx` is the issue number on github.

### Pull Requests
Once development is complete on a branch, developers need to request a Pull Request (PR) on github.  The PR description should include the issue(s) numbers in the descriptions in the form "#xxx" so that the PR is linked to the issue(s).

### Merge vs Rebase
Users should use `rebase` when merging changes from master into their local branch.  However, this should only be done on none-shared branches.  When multiple users are developing on a branch, merge strategy should be used.  If the concept of merge vs rebase sounds foreign, just use the default merge strategy as it is safer.

## 2. Code Style
### Readability
We should emphasize readability over "cleverness".  Simple multi-line code is better than a single code that is difficult to decipher.

### Comments
Each major class and function should have a block comment.  Scala classes/functions should use `scaladoc` formatted block comments, while python classes/functions should use standard python `__doc__` strings.

### Code Format
To increase code readability, we should have a consistent code style for both scala and python code.
#### Scala
We use `scalafmt` for scala code formatting.  Once PR #668 (https://github.com/TresAmigosSD/SMV/pull/668) is merged into master, then the formatting will be done automatically on compile.
#### Python
We should follow PEP8 for python styling (See https://www.python.org/dev/peps/pep-0008/).  One exception is use of CamelCase for function names to match the function names on the Scala side.

When using Atom as editor, please install `linter-flake8` package. It will automatically
install dependencies. Also it need you to install flake8 on Python, so need to do
`pip install flake8` in terminal.

### Abstract decorators
Python's `abc` package provides both `abstractmethod` and `abstractproperty` decorators.
When we define user-interface abstract classes (`SmvDataSet`), we should always use
`abstractmethod` to decorate mandatory interface methods.

For example:
```python
class SmvCsvFile(...):
  @abstractmethod
  def path(self):
    pass
```


## Testing
TBD
### Scala Unit Test
### Python Unit Test
### Integration Test

## Release Train
SMV will use a release train model for releases.  There will be one release branch for each of the supported major versions plus master.  Master branch will be the latest supported release train branch.
For example, we may have a `1.5` and `1.6` branches with SMV version `v1.5.2.6` and `v1.6.1.3` on the respective branches.  Master branch will be supporting the `2.1` releases and has a current SMV version of `v2.1.1.2`.

New functionality should be added to the master branch and only ported back to the maintenance branches as needed.

We will not support patch releases or hot fixes.  Bug fixes and new features on a maintenance branch will always be released as part of the latest release in a given maintenance branch.
