package clairem.data

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._
import magnolify.bigquery._

object RunnerV2Test {
  case class BqProjection(names: List[String])

  val BqType = TableRowType[BqProjection]

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)

    val genomeData = sc.bigQueryTable(
      Table.Spec("bigquery-public-data.human_genome_variants.1000_genomes_phase_3_optimized_schema_variants_20150220")
    )

    genomeData
      .map(BqType.from)
      .flatMap(_.names)
      .countByValue
      .topByKey(args.int("n"))(Ordering.by(identity))
      .flatMapValues(identity) // flatten result
      .map { case (name, count) => s"{ \"name\": \"$name\", \"count\": \"$count\""}

    sc.run()
  }
}
