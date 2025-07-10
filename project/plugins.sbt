val patternBase =
  "org/broadinstitute/monster/[module](_[scalaVersion])(_[sbtVersion])/[revision]"

val publishPatterns = Patterns()
  .withIsMavenCompatible(false)
  .withIvyPatterns(Vector(s"$patternBase/ivy-[revision].xml"))
  .withArtifactPatterns(Vector(s"$patternBase/[module]-[revision](-[classifier]).[ext]"))

resolvers += "Google Artifact Repository" at "https://us-central1-maven.pkg.dev/dsp-artifact-registry/libs-release/"

// useful when testing ingest-utils locally
resolvers += Resolver.mavenLocal

addSbtPlugin("org.broadinstitute.monster" % "ingest-sbt-plugins" % "2.1.12")
