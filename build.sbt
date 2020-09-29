import _root_.io.circe.Json

lazy val `hca-ingest` = project
  .in(file("."))
  .aggregate(`hca-schema`, `hca-transformation-pipeline`, `hca-orchestration-workflow`)
  .settings(publish / skip := true)

lazy val `hca-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeTablePackage := "org.broadinstitute.monster.hca.jadeschema.table",
    jadeTableFragmentPackage := "org.broadinstitute.monster.hca.jadeschema.fragment",
    jadeStructPackage := "org.broadinstitute.monster.hca.jadeschema.struct"
  )

lazy val `hca-transformation-pipeline` = project
  .in(file("transformation"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .settings(
    resolvers += "jitpack".at("https://jitpack.io"),
    libraryDependencies ++= Seq(
      "com.lihaoyi" %% "requests" % "0.5.1",
      "io.circe" %% "circe-json-schema" % "0.1.0"
    )
  )
  .dependsOn(`hca-schema`)

lazy val `hca-orchestration-workflow` = project
  .in(file("orchestration"))
  .enablePlugins(MonsterHelmPlugin)
  .settings(
    helmChartOrganization := "DataBiosphere",
    helmChartRepository := "hca-ingest"
  )
