package com

import cats.data.Reader
import com.data.model.TaxiData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}

package object data {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder
    .appName("taxi-data-analysis")
    .master("local[*]")
    .getOrCreate()

  val taxiEncoderSchema = Encoders.product[TaxiData].schema

  val zoneSchema = new StructType()
    .add("h3_index", StringType)
    .add("zone", StringType)
    .add("borough", StringType)
    .add("location_id", IntegerType)

  val rtbfSchema = new StructType()
    .add("taxi_id", IntegerType)

  type SparkSessionReader[A] = Reader[SparkSession, A]

  object SparkSessionReader {
    def apply[A](f: SparkSession => A): SparkSessionReader[A] =
      Reader[SparkSession, A](f)

    def lift[A](x: A): SparkSessionReader[A] =
      Reader[SparkSession, A](_ => x)
  }
}
