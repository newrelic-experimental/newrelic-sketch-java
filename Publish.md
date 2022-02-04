# Publishing to Maven

NrSketch publishes artifacts to Maven, in the "com.newrelic" group. To publish a new version, use this procedure:

* Update `nrSketchVersion` in [gradle.properties](gradle.properties)
* Update [RELEASES.md](RELEASES.md)
* Run "./gradlew publish", which publishes to a Maven staging repo, because the "com.newrelic" group is configured
  to go to staging first. See
  [staging process](https://help.sonatype.com/repomanager2/staging-releases/staging-overview) for more info.
* Log onto [Sonatype repository manager](https://oss.sonatype.org/index.html#stagingRepositories) using your
  Sonatype id. Then verify content of the staging repo.
* If staging repo content appears good, "close" the repo to make it visible to test apps.
* Test the staging repo by loading nrSketch from a test app.
* Once testing passes, "release" the staging repo. This will make the repo visible in the standard Maven space. It 
  may take 5 to 10 minutes for the new version to appear in the Maven space. You can monitor the new version's 
  visibility at
  https://repo.maven.apache.org/maven2/com/newrelic/nrsketch/ 
* Merge nrSketch version and release doc changes to the main branch on GitHub
* Create release tag in GitHub

"./gradlew publish" is wired to publish to Maven. You must set the following environment variables before running it:
* `SONATYPE_USERNAME` and `SONATYPE_PASSWORD`. The Sonatype user id must have upload permission to the "com.
  newrelic" group. A user id may be created at [Sonatype](https://issues.sonatype.org/secure/Dashboard.jspa). Then
  you may request access by making a comment on this [ticket](https://issues.sonatype.org/browse/OSSRH-4818).
* `GPG_SECRET_KEY` and `GPG_PASSWORD`. The key may be created via this
  [process](https://central.sonatype.org/publish/requirements/gpg/). Load the output of `gpg -armor
  --export-secret-keys <keyId>` into `GPG_SECRET_KEY` as a multi-line value.

Note that some versions of GnuPG may default to an encryption algorithm other than
  "rsa" on the "--generate-key" option. If so, use "--full-generate-key" to specify "rsa", because Maven may not
  support other algorithms. With unsupported algorithm, Maven may just give a vague "cannot read key" error during 
publish.

To test nrSketch on the staging repo, you may use these rules in the test app's build.gradle file to load from the
staging space rather than the usual release space.
```
repositories {
    //mavenCentral()
    maven {
        url "https://oss.sonatype.org/content/repositories/staging"
    }
}
```