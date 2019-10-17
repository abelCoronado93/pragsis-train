package com.pragsis.train

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ScalaAggrs {

  def main(args: Array[String]) {

    /*  ----- GET CURRENT TIME FOR BATCH DAILY PROCESS -----
    val year = new SimpleDateFormat("yyyy").format(Calendar.getInstance.getTime).toInt
    val month = new SimpleDateFormat("MM").format(Calendar.getInstance.getTime).toInt
    val day = new SimpleDateFormat("dd").format(Calendar.getInstance.getTime).toInt
    */

    val year = 2018
    val month = 10
    val day = 1

    val sc = new SparkContext(new SparkConf().setAppName("Aggregations"))
    val sqlContext = new SQLContext(sc)

    val queryCSV = s"SELECT *, hour(from_unixtime(timestamp)) AS hour FROM bd_prueba.ut_data where year=$year AND month=$month AND day=$day"
    val dfCSV = new HiveContext(sc).sql(queryCSV)

    // Conteo de distintos de TODOS los campos
    /*
    val exprs = dfCSV.columns.map(countDistinct(_))
    val df2 = dfCSV.groupBy("hour").agg(exprs.head, exprs.tail: _*)
    */

    // Porcentaje de Trues (1s) en una columna específica
    var register = true
    var dfPurge = dfCSV
    for (col_name <- dfCSV.columns){
      if (dfCSV.schema(col_name).dataType.typeName == "boolean"){
        var dfPercent = dfCSV.groupBy("hour").
          agg(sum(col(col_name).cast("integer")).alias(s"$col_name"+"_SumTrues"),
              mean(col(col_name).cast("integer")).multiply(100).alias(s"$col_name"+"_percentage"))
        if (register) {
          dfPercent.registerTempTable("joinMaster")
          register = false
        } else {
          dfPercent.registerTempTable("dfPercent")
          var total = sqlContext.sql("select * from joinMaster")
          total.join(dfPercent, "hour").registerTempTable("joinMaster")
        }
      }
      if (dfCSV.schema(col_name).dataType.typeName != "integer") dfPurge = dfPurge.drop(col_name)
    }

    val meanDF = dfPurge.groupBy("hour").mean()
    meanDF.write.
      format("csv").
      mode(SaveMode.Overwrite).
      save(s"/user/master/grupo1/comsumption/mean/year=$year/month=$month/day=$day")
    meanDF.registerTempTable("meanMaster")
    sqlContext.sql("create external table bd_prueba.mean location '/user/master/grupo1/comsumption/mean/' as select * from meanMaster")

    val failures = sqlContext.sql("select * from joinMaster")
    failures.write.
      format("parquet").
      mode(SaveMode.Overwrite).
      save(s"/user/master/grupo1/comsumption/failures/year=$year/month=$month/day=$day")
    sqlContext.sql("create external table bd_prueba.failures location '/user/master/grupo1/comsumption/failures/' as select * from joinMaster")
  }
}

// Testing : dfCSV = dfCSV.withColumn("sdi_m1_m_sts1w", dfCSV.col("sdi_m1_m_sts1w").cast("boolean"))