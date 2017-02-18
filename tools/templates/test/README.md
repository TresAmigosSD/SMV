# Test project for SMV
Sample project to run integration tests against

## Test Stages
Each test consists of a distinct module dependency scenario which contains 1 to 2 stages.

### Test 1: test1
Scala depends on Scala within same stage

### Test 2: test2
Python depends on Python within same stage

### Test 3: test3, test3_1
Scala depends on Scala in different stage

### Test 4: test4, test4_1
Python depends on Python in different stage

### Test 5: test5
Scala depends on Python

### Test 6: test6
Python depends on Scala
