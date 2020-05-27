import _root_.io.circe.Json

lazy val `hca-ingest` = project
  .in(file("."))
  .aggregate(`hca-schema`, `hca-transformation-pipeline`)
  .settings(publish / skip := true)

lazy val `hca-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeTablePackage := "org.broadinstitute.monster.hca.jadeschema.table",
    jadeStructPackage := "org.broadinstitute.monster.hca.jadeschema.struct"
  )

lazy val `hca-transformation-pipeline` = project
  .in(file("transformation"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .dependsOn(`hca-schema`)

lazy val `hca-orchestration-workflow` = project
  .in(file("orchestration"))
  .enablePlugins(MonsterHelmPlugin)
  .settings(
    helmChartOrganization := "DataBiosphere",
    helmChartRepository := "hca-ingest",
    helmInjectVersionValues := { (baseValues, version) =>
      val schemaVersionValues = Json.obj(
        "argoTemplates" -> Json.obj(
          "diffBQTable" -> Json.obj(
            "schemaImageVersion" -> Json.fromString(version)
          )
        )
      )
      baseValues.deepMerge(schemaVersionValues)
    }
  )
