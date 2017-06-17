# Creating an SMV Release

For this doc, assume we are tagging/building SMV release # 1.2.3.4.  User should substitute the appropriate version.

The process has been largely automated.  The `smv-release` script will take care of most steps required to build/test/deploy a release.

## Usage:
```shell
$ ./admin/smv-release [--new-branch] [--latest] -g github_user:github_token -d docker_user  docker_password new_version
```

* `--new-branch` : used if this the first release on a new branch (user must edit the ghpages index manually)
* `--latest` : if specified, the docker hub image is marked as latest as well as the specific version.
* `github_user','github_token` : github API access tokens
* `docker_user`,`docker_password` : docker hub user name/password.
* `new_version` : version to release.  must be of the form "n.n.n.n"


## release with module updates
It is sometimes necessary to release with some additional python modules (that have been added to requirements.txt).
However, the latest "tresamigos/smv:latest" docker image will not have these changes.  It is easiest to just modify
the `Dockerfile` to add the explicit `"RUN pip install xxx"` and run `docker build -t tresamigos/smv:latest .` to
build a new smv image and use that for the release (may need to revert the changes to allow the release process
to continue).
