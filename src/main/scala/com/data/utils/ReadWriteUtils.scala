package com.data.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

object ReadWriteUtils {

  def readCSV(spark:SparkSession, schema:StructType, path: String)(implicit logger:Logger): DataFrame = {
   val df = spark
     .sqlContext
     .read
     .format("csv")
     .option("sep", ",")
     .option("header", "true")
     .schema(schema)
     .load(path)
    df
  }


  def writeCSV(df: DataFrame, path: String)(implicit logger:Logger): Unit = {
    df.coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save(path)
    logger.info(s"CSV file written at $path")
  }

}
