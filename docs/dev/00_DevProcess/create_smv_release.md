# Creating an SMV Release

For this doc, assume we are tagging/building SMV release # 1.2.3.4.  User should substitute the appropriate version.
The process below will be automated further in future releases.

## 1. tag the source
```
$ git tag -a v1.2.3.4 -m "SMV release 1.2.3.4 (mm/dd/yyyy)"
$ git push origin v1.2.3.4
```

## 2. update docker hub to build with latest tag
TBD

## 3. create the release image
```
$ release_smv.sh 1.2.3.4
```
Substitute the real release version for "1.2.3.4"

## 4. Create github release
* click on "draft a new release" on https://github.com/TresAmigosSD/SMV/releases
* select the tag version created in step 1 above.
* attach the binary created in step 3 to the release. (can also be done after the release has been created)
