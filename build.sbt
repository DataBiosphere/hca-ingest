//import _root_.io.circe.Json
import org.broadinstitute.monster.sbt.model.JadeIdentifier

lazy val `hca-ingest` = project
  .in(file("."))
  .aggregate(`hca-schema`, `hca-transformation-pipeline`)
  .settings(publish / skip := true)

lazy val `hca-schema` = project
  .in(file("schema"))
  .enablePlugins(MonsterJadeDatasetPlugin)
  .settings(
    jadeDatasetName := JadeIdentifier
      .fromString("broad_dsp_hca")
      .fold(sys.error, identity),
    jadeDatasetDescription := "Mirror of HCA archive, maintained by Broad's Data Sciences Platform",
    jadeTablePackage := "org.broadinstitute.monster.hca.jadeschema.table",
    jadeStructPackage := "org.broadinstitute.monster.hca.jadeschema.struct"
  )

lazy val `hca-transformation-pipeline` = project
  .in(file("transformation"))
  .enablePlugins(MonsterScioPipelinePlugin)
  .dependsOn(`hca-schema`)

//lazy val `hca-orchestration-workflow` = project
//  .in(file("orchestration"))
//  .enablePlugins(MonsterHelmPlugin)
//  .settings(
//    helmChartOrganization := "DataBiosphere",
//    helmChartRepository := "hca-ingest",
//    helmInjectVersionValues := { (baseValues, version) =>
//      val schemaVersionValues = Json.obj(
//        "argoTemplates" -> Json.obj(
//          "diffBQTable" -> Json.obj(
//            "schemaImageVersion" -> Json.fromString(version)
//          )
//        )
//      )
//      baseValues.deepMerge(schemaVersionValues)
//    }
//  )
