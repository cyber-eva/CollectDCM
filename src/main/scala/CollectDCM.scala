import org.apache.spark.sql
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.SparkSession

object CollectDCM {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder.appName("ChainBuilder").getOrCreate()
    import spark.implicits._

    val input_folder  = args(0)
    val output_folder = args(1)

    val data = spark.read.
      format("csv").
      option("inferSchema","false").
      option("mergeSchema","true").
      load(input_folder)

    val data_work = data.select(
      $"utm_source".cast(sql.types.StringType),
      $"utm_medium".cast(sql.types.StringType),
      $"utm_campaign".cast(sql.types.StringType),
      $"utm_content".cast(sql.types.StringType),
      $"utm_term".cast(sql.types.StringType),
      $"profile_id".cast(sql.types.StringType),
      $"ad_id".cast(sql.types.StringType),
      $"creative_id".cast(sql.types.StringType),
      $"date".cast(sql.types.LongType),
      $"hour".cast(sql.types.StringType),
      $"impressions".cast(sql.types.LongType),
      $"views".cast(sql.types.LongType),
      $"clicks".cast(sql.types.LongType)
    )

    val data_date = data_work.withColumn("date",concat_ws(":",$"date",$"hour"))

    val data_agg = data_date.
      groupBy(
        $"utm_source",
        $"utm_medium",
        $"utm_campaign",
        $"utm_content",
        $"utm_term",
        $"profile_id",
        $"ad_id",
        $"creative_id",
        $"date"
      ).agg(
      sum($"views").as("views"),
      sum($"clicks").as("clicks"),
      sum($"impressions").as("impressions")
    )

    data_agg.
      write.format("csv").
      option("header","true").
      mode("overwrite").
      save(output_folder)

  }


}
