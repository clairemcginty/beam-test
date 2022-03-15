package clairem.data

import com.spotify.scio._
import com.spotify.scio.extra.json._
import com.spotify.scio.options.ScioOptions
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection}

/*
To run:

sbt "runMain clairem.data.JoinTest --runner=DataflowRunner --region=us-central1 --project=[[PROJECT]]"
*/
object JoinTest {

  case class Edit(title: String, id: String, timestamp: Long, num_characters: Long)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    args.getOrElse("impl", "beam") match {
      case "scio" => scioImpl(sc)
      case "beam" => beamImpl(sc)
      case _: String => ???
    }
    sc.run()
  }

  def beamImpl(sc: ScioContext): Unit = {
    // Contrive data to get large key groups
    val lhs: PCollection[KV[String, String]] = sc
      .jsonFile[Edit]("gs://dataflow-samples/wikipedia_edits/wiki_data-0000000001*")
      .map { edit => KV.of((edit.num_characters % 250).toString, edit.id) } // % 1000 produced key groups too small to trigger error
      .internal // to PCollection
      .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))

    val rhs: PCollection[KV[String, String]] = sc
      .jsonFile[Edit]("gs://dataflow-samples/wikipedia_edits/*")
      .map { edit => KV.of((edit.timestamp % 250).toString, edit.title) } // % 1000 produced key groups too small to trigger error
      .internal // to PCollection
      .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))

    KeyedPCollectionTuple
      .of[String, String]("lhs", lhs)
      .and[String]("rhs", rhs)
      .apply(CoGroupByKey.create[String])
      .apply(ParDo.of(new DoFn[KV[String, CoGbkResult], String] {
        @ProcessElement
        private[data] def processElement(
          c: DoFn[KV[String, CoGbkResult], String]#ProcessContext
        ): Unit = {
          val input = c.element()
          val rhsIterable = input.getValue.getAll[String]("rhs")
          val lhsIterable = input.getValue.getAll[String]("lhs")
          rhsIterable.iterator().forEachRemaining { r =>
            lhsIterable.iterator().forEachRemaining(l => {
              c.output(s"$l-$r")
            })
          }
        }
    }))
  }

  def scioImpl(sc: ScioContext): Unit = {
    val rhs = sc
      .jsonFile[Edit]("gs://dataflow-samples/wikipedia_edits/*")
      .map { edit => ((edit.timestamp % 250).toString, edit.title) }

    val lhs = sc
      .jsonFile[Edit]("gs://dataflow-samples/wikipedia_edits/wiki_data-00000000001*")
      .map { edit => ((edit.num_characters % 250).toString, edit.id) }

    lhs.join(rhs).map { case (k, (l, r)) =>
      s"$l-$r"
    }
  }
}
