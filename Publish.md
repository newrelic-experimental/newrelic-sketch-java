# Publishing to Maven

**IMPORTANT: By default, Gradle uses daemon processes to run builds. Environment variables are read only on daemon startup. 
Building a task does NOT use the environment of the task's invocation context. 
To refresh environment variables, run "gradlew --stop" or disable using daemon for builds.**

NrSketch publishes artifacts to Maven, in the "com.newrelic" group. To publish a new version, use this procedure:

* Update `nrSketchVersion` in [gradle.properties](gradle.properties)
* Update [RELEASES.md](RELEASES.md)
* Run `./gradlew publish`, which publishes to a Maven staging repo, because the "com.newrelic" group is configured
  to go to staging first. See
  [staging process](https://help.sonatype.com/repomanager2/staging-releases/staging-overview) for more info.
  * The publish task must run with certain environment variables set. See next section for more info.
  * The publish task also creates a local copy in the "build/artifactory" directory. To publish to local directory 
    only, run "./gradlew publishMavenJavaPublicationToLocalRepository". The Sonatype environment variables are not 
    needed for local publish. The GPG (GnuPG) signing environment variables are still required.
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

`./gradlew publish` is wired to publish to Maven. You must set the following environment variables before running it:
* `SONATYPE_USERNAME` and `SONATYPE_PASSWORD`. These are used to access Maven repo. The Sonatype user id must have 
  upload permission to the "com.newrelic" group. A user id may be created at
  [Sonatype](https://issues.sonatype.org/secure/Dashboard.jspa). Then
  you may request "com.newrelic" access by making a comment on this 
  [ticket](https://issues.sonatype.org/browse/OSSRH-4818). In a typical scenario, a New Relic employee first acquires 
  permission via a channel internal to New Relic, then creates the Sonatype id using a newrelic.com email address, then
  requests permission on the Sonatype ticket. When the "com.newrelic" administrators see the request, 
  they already have the context.
* `GPG_SECRET_KEY` and `GPG_PASSWORD`. These are used to sign the artifacts. The key may be created via this
  [process](https://central.sonatype.org/publish/requirements/gpg/). Load the output of `gpg -armor
  --export-secret-keys <keyId>` into `GPG_SECRET_KEY` as a multi-line value.

The signing gradle plugin definitely supports the "rsa" encryption method. Other methods may not be supported.
Some [docs](https://central.sonatype.org/publish/requirements/gpg/) show using the GnuPG "--generate-key" option to 
create new keys. However, at least on GnuPG version 2.3.4, the "--generate-key" option does not give users the option
to choose an encryption method and the key is created with a method other than "rsa". Unfortunately this other method
is not supported by gradle signing plugin, and gradle gives a vague error like "cannot read key". 
Thus it is recommended to use the GnuPG "--full-generate-key" option to create the key. The full 
option allows you to specify the "rsa" algorithm, and to specify an expiration duration of the key ("--generate-key" 
defaults to 2 years, though you can change it after key creation).

To test nrSketch on the staging repo, you may use a staging repo rule in the test app's build.gradle file. Example:
```
repositories {
    maven {
        url "https://oss.sonatype.org/content/repositories/staging"
    }
}
```
