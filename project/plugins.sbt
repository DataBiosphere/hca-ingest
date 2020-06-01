val sbtPluginsVersion = "1.0.0"

val patternBase =
  "org/broadinstitute/monster/[module](_[scalaVersion])(_[sbtVersion])/[revision]"

val publishPatterns = Patterns()
  .withIsMavenCompatible(false)
  .withIvyPatterns(Vector(s"$patternBase/ivy-[revision].xml"))
  .withArtifactPatterns(Vector(s"$patternBase/[module]-[revision](-[classifier]).[ext]"))

resolvers += Resolver.url(
  "Broad Artifactory",
  new URL("https://broadinstitute.jfrog.io/broadinstitute/libs-release/")
)(publishPatterns)

addSbtPlugin("org.broadinstitute.monster" % "sbt-plugins-jade" % sbtPluginsVersion)
addSbtPlugin("org.broadinstitute.monster" % "sbt-plugins-scio" % sbtPluginsVersion)
addSbtPlugin("org.broadinstitute.monster" % "sbt-plugins-helm" % sbtPluginsVersion)
