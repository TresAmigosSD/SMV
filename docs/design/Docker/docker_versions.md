# SMV Docker Versions

We currently use ":latest" tag for both `smv-core` and `smv` docker images.  Users should ideally depend on a versioned tag as latest is always changing.

# smv-core
`smv-core` version should not be tied to the SMV release number.  Instead, its tag should be based on the tool versions it has.  For example:
* `core_jdk7_py27` : JDK 7 with python 2.7
* `core_jdk7_py35` : JDK 7 with python 3.5
* `core_jdk8_py35` : JDK 8 with python 3.5

The `smv` image would then depend on a specific version of the `smv-core` image rather than latest.
To trigger the build of the core image, we will tag the release with the string `core_*` which will allow docker hub to build a new release of core.

**NOTE**: This is ok as long as we only need a single version of a given `core_*` tag.  Docker hub makes it hard to replace tags for automatic builds.  We may need to investigate switching to push builds instead of github automatic builds.

# smv
For the main `smv` docker image, we would tag the release the same as the SMV release.  Docker hub with then trigger the build off of the `v1.2.3.4` tag format.
