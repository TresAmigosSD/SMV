# Application Deployment

Environments and policies differ widely from one deployment to another so it is nearly impossible to capture all scenarios within SMV.  Instead, a sample deployment distribution script is included with SMV as a starting point. The script is included under the SMV home directory under `tools/dist/makedist.sh`

The sample script creates a deployment bundle that can be deployed on the production environment.  The bundle will include the SMV code **and** project code under a single directory so the user would not need to install SMV on production servers.

The sample script will also include a runner script `run.sh` into the generated bundle.  The `run.sh` script has some environment variables that affect the running of application (e.g. yarn queue, driver memory, etc).  These environment variables can be set by the dev-ops engineer or overridden in production.
