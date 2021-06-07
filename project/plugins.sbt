val patternBase =
  "org/broadinstitute/monster/[module](_[scalaVersion])(_[sbtVersion])/[revision]"

val publishPatterns = Patterns()
  .withIsMavenCompatible(false)
  .withIvyPatterns(Vector(s"$patternBase/ivy-[revision].xml"))
  .withArtifactPatterns(Vector(s"$patternBase/[module]-[revision](-[classifier]).[ext]"))

//resolvers += Resolver.url(
//  "Broad Artifactory",
//  new URL("https://broadinstitute.jfrog.io/broadinstitute/libs-release/")
//)(publishPatterns)


resolvers += Resolver.mavenLocal


addSbtPlugin("org.broadinstitute.monster" % "ingest-sbt-plugins" % "2.1.10-0-0f366386-20210607-0900-SNAPSHOT")
