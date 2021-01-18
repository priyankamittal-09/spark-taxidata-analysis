package com.data

import java.sql.Timestamp
import cats.Id
import com.data.model.{DropOffTaxiData, PickDropTaxiData, PickupTaxiData, TaxiData}
import com.data.utils.ReadWriteUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{conv, lower}
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.specs2.Specification
import org.specs2.matcher.MatchResult
import org.specs2.specification.AfterAll
import org.specs2.specification.core.SpecStructure
import scala.collection.JavaConverters._
import scala.collection.mutable

class MainSpec extends Specification with AfterAll {
  def is: SpecStructure = sequential ^
    s2"""
      taxi data analysis
      =============

        Test Case 1 Passed $readTaxiDataTest

        Test Case 2 Passed $readZonesDataTest

        Test Case 3 Passed $filteredTaxiDataTest

        Test Case 4 Passed $filteredTaxiLargeDataTest

        Test Case 5 Passed $calculateFareTest


    """

  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val logger = Logger.getLogger(this.getClass)

  val spark: SparkSession = SparkSession
    .builder
    .appName("MainSpec")
    .master("local[*]")
    .getOrCreate()

  logger.info("Test Cases Started")

  def afterAll: Unit = {
    spark.stop()
  }

  import spark.implicits._

  val sample_taxi_data: Dataset[TaxiData] =
    List(
        TaxiData(2, Timestamp.valueOf("2015-02-12 17:34:27"), Timestamp.valueOf("2015-02-12 17:50:15"), 2, 1.29, -73.9767074584961, 40.739295959472656, 1, "N", -73.988525390625, 40.74862289428711, 1, 10.5, 1.0, 0.5, 3.08, 0.0, 0.3, 15.38, "8b2a100d2a01fff", "8b2a100d2c76fff", 11128 ),
        TaxiData(2, Timestamp.valueOf("2015-02-12 17:34:27"), Timestamp.valueOf("2015-02-12 17:50:15"), 2, 1.29, -73.9767074584961, 40.739295959472656, 1, "N", -73.988525390625, 40.74862289428711, 1, 10.5, 1.0, 0.5, 3.08, 0.0, 0.3, 15.38, "8b2a100d2a01fff", "8b2a100d2c76fff", 11120 ),
        TaxiData(2, Timestamp.valueOf("2015-02-12 17:34:27"), Timestamp.valueOf("2015-02-12 17:50:15"), 2, 1.29, -73.9767074584961, 40.739295959472656, 1, "N", -73.988525390625, 40.74862289428711, 1, 10.5, 1.0, 0.5, 3.08, 0.0, 0.3, 15.38, "8b2a100d2a01fff", "8b2a100d2c76fff", 11138 )
  ).toDS()

  val sample_filtered_taxi_data: Dataset[TaxiData] =
    List(
      TaxiData(2, Timestamp.valueOf("2015-02-12 17:34:27"), Timestamp.valueOf("2015-02-12 17:50:15"), 2, 1.29, -73.9767074584961, 40.739295959472656, 1, "N", -73.988525390625, 40.74862289428711, 1, 10.5, 1.0, 0.5, 3.08, 0.0, 0.3, 15.38, "8b2a100d2a01fff", "8b2a100d2c76fff", 11128 ),
      TaxiData(2, Timestamp.valueOf("2015-02-12 17:34:27"), Timestamp.valueOf("2015-02-12 17:50:15"), 2, 1.29, -73.9767074584961, 40.739295959472656, 1, "N", -73.988525390625, 40.74862289428711, 1, 10.5, 1.0, 0.5, 3.08, 0.0, 0.3, 15.38, "8b2a100d2a01fff", "8b2a100d2c76fff", 11138 )
    ).toDS()

  val sample_zones_data = Seq(
    ("626740350040485887", "Newark Airport", "EWR", 1),
    ("626740350041087999", "Newark Airport", "EWR", 1)
  ).toDF("h3_index", "zone", "borough", "location_id")
    .withColumn("h3_index", lower(conv($"h3_index",10,16)))


  val sample_taxiFareData = Seq(
    (21, 64.78, 2),
    (31, 113.96, 1)
  ).toDF("trip_distance", "average_total_fare", "number_of_trips")



  def readTaxiDataTest: MatchResult[mutable.Buffer[TaxiData]] = {
    logger.info("Test Case 1: Should read input parquet data file correctly")
    val path = "src/test/resources/ny_taxi_data_small/ny_taxi"
    val result: Id[Dataset[TaxiData]] = Main.readTaxiData(path).run(spark)
    logger.info("Sample Taxi Data")
    sample_taxi_data.printSchema()
    sample_taxi_data.show()

    val calculated = result.collectAsList()
    val expected = sample_taxi_data.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

  def readZonesDataTest: MatchResult[mutable.Buffer[Row]] = {
    logger.info("Test Case 2: Should read zones csv data file correctly")
    val path = "src/test/resources/ny_taxi_data_small/ny_zones/ny_taxi_zones.csv"
    val result = Main.readZonesData(path).run(spark)
    logger.info("Sample Zones Data")
    sample_zones_data.printSchema()
    sample_zones_data.show()

    val calculated = result.collectAsList()
    val expected = sample_zones_data.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

  def filteredTaxiDataTest: MatchResult[mutable.Buffer[TaxiData]] = {
    logger.info("Test Case 3: Should filter out ids of taxi drivers who would like to be forgotten")
    val path = "src/test/resources/ny_taxi_data_small/ny_taxi_rtbf/rtbf_taxies.csv"
    val result = Main.filteredTaxiData(sample_taxi_data, Main.readRightToBeForgottenData(path).run(spark)).run(spark)
    logger.info("Sample Filtered Data")
    sample_filtered_taxi_data.printSchema()
    sample_filtered_taxi_data.show()

    val calculated = result.collectAsList()
    val expected = sample_filtered_taxi_data.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }



  def filteredTaxiLargeDataTest: MatchResult[mutable.Buffer[TaxiData]] = {
    logger.info("Test Case 4: Should filter out ids of taxi drivers who would like to be forgotten")
    val data_path = "src/test/resources/ny_taxi_data_large/ny_taxi"
    val rtbf_path = "src/test/resources/ny_taxi_data_large/ny_taxi_rtbf/rtbf_taxies.csv"
    val result = Main.filteredTaxiData(Main.readTaxiData(data_path).run(spark), Main.readRightToBeForgottenData(rtbf_path).run(spark)).run(spark)

    val calculated = result.collectAsList()
    val expected = ReadWriteUtils.readCSV(spark, taxiEncoderSchema, "src/test/resources/ny_taxi_data_large/ny_taxi_filtered_data").as[TaxiData].collectAsList()
    logger.debug(s"Calculated: $calculated")
    logger.debug(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

  def calculateFareTest: MatchResult[mutable.Buffer[Row]] = {
    import spark.implicits._
    logger.info("Test Case 5: Should calculate the average total fare for each trip_distance rounded to 0 decimal places and the number of trips correctly")
    val outputPath = "src/test/resources/ny_taxi_insights/insight1"
    val encoderSchema = Encoders.product[PickDropTaxiData].schema
    val sample_pick_drop_taxi_data = ReadWriteUtils.readCSV(spark, encoderSchema, "src/test/resources/ny_taxi_data_large/ny_taxi_pickdrop_data/pick-drop.csv").as[PickDropTaxiData]
    val result = Main.calculateFare(sample_pick_drop_taxi_data, outputPath).run(spark).filter($"trip_distance" >20 )
    val calculated = result.collectAsList()
    val expected = sample_taxiFareData.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

}

