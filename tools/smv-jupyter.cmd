set PYSPARK_DRIVER_PYTHON=jupyter
set PYSPARK_DRIVER_PYTHON_OPTS="notebook"
set PYTHONPATH=%PATHONPATH%;%SMV_HOME%\python;src\main\python
set PYTHONSTARTUP=smv_pyshell_init.py

set APP_JAR=%SMV_HOME%\target\scala-2.10\smv-1.5-SNAPSHOT-jar-with-dependencies.jar

powershell -Command "(Get-Content %SMV_HOME%\tools\conf\smv_pyshell_init.py.template) | ForEach-Object { $_ -replace '_SMV_ALL_ARGS_', '' } | Set-Content smv_pyshell_init.py"

# Pass through the options from `smv-jupyter` invocation through to `smv-pyshell`
# This will allow the user to specify pyspark options like:
# `smv-jupyter -- --master=yarn-client --num-executors=10`
# `smv-jupyter -- --conf="spark.driver.maxResultSize=0"`
pyspark --jars %APP_JAR% --driver-class-path %APP_JAR%  
