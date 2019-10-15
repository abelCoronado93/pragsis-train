package com.pragsis.train

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import scala.util.Try
import java.util.Calendar
import java.text.SimpleDateFormat

object SparkSQL {

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  def main(args: Array[String]) {

    /*  ----- GET CURRENT TIME FOR BATCH DAILY PROCESS -----
    val year = new SimpleDateFormat("yyyy").format(Calendar.getInstance.getTime).toInt
    val month = new SimpleDateFormat("MM").format(Calendar.getInstance.getTime).toInt
    val day = new SimpleDateFormat("dd").format(Calendar.getInstance.getTime).toInt
    */

    val year = 2018
    val month = 10
    val day = 1

    val sc = new SparkContext(new SparkConf().setAppName("Enrichment"))
    val sqlContext = new SQLContext(sc)

    var queryCSV = s"SELECT * FROM bd_prueba.ut_data where year=$year AND month=$month AND day=$day"

    var dfCSV = new HiveContext(sc).sql(queryCSV)

    val dfParquet = sqlContext.read.parquet("/user/master/grupo1/dictionary_data/*.parquet")

    var dict = sc.broadcast(dfParquet.rdd.collect())
    dict.value
      .filter(x => hasColumn(dfCSV, x(0).toString))
      .foreach(x => {
        dfCSV = dfCSV
          .withColumn(x(0).toString, dfCSV.col(x(0).toString).cast(x(2).toString)) // Casting
          .withColumnRenamed(x(0).toString, x(1).toString + "_enrich") // Rename
      })

    dfCSV.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(s"/user/master/grupo1/master/parquet/year=$year/month=$month/day=$day")
  }
}

