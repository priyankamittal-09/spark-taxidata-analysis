package com

import cats.data.Reader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, DoubleType, IntegerType, StringType, StructField, StructType}

package object data {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder
    .appName("taxi-data-analysis")
    .master("local[*]")
    .getOrCreate()

  val taxiSchema = StructType(Array(
    StructField("vendor_id",IntegerType),
    StructField("tpep_pickup_datetime",DataTypes.TimestampType),
    StructField("tpep_dropoff_datetime",DataTypes.TimestampType),
    StructField("passenger_count",IntegerType),
    StructField("trip_distance",DoubleType),
    StructField("pickup_longitude",DoubleType),
    StructField("pickup_latitude",DoubleType),
    StructField("rate_code_id",IntegerType),
    StructField("store_and_forward",StringType),
    StructField("dropoff_longitude",DoubleType),
    StructField("dropoff_latitude",DoubleType),
    StructField("payment_type",IntegerType),
    StructField("fare_amount",DoubleType),
    StructField("extra",DoubleType),
    StructField("mta_tax",DoubleType),
    StructField("tip_amount",DoubleType),
    StructField("tolls_amount",DoubleType),
    StructField("improvement_surcharge",DoubleType),
    StructField("total_amount",DoubleType),
    StructField("pickup_h3_index",StringType),
    StructField("dropoff_h3_index",StringType),
    StructField("taxi_id",IntegerType)))

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
