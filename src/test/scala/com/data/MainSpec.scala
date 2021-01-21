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

        Test Case 6 Passed $calculateNumberOfPickUpsPerZoneTest

        Test Case 7 Passed $calculateNumberOfPickUpsPerBoroughTest

        Test Case 8 Passed $calculateDropOffsPerZoneStatsTest

        Test Case 9 Passed $calculateDropOffsPerBoroughStatsTest

        Test Case 10 Passed $calculateTopDropoffZonePerPickupZoneTest

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

  val sample_numberOfPickUpsPerZoneDF = Seq(
    ("Upper West Side South", 1),
    ("Little Italy/NoLiTa", 1),
    ("Union Sq", 1),
    ("Penn Station/Madison Sq West", 1)
  ).toDF("zone", "number_of_pickups")

  val sample_numberOfPickUpsPerBoroughDF = Seq(
    ("Manhattan", 8)
  ).toDF("borough", "number_of_pickups")

  val sample_dropoffsPerZoneStatsDF = Seq(
    ("Alphabet City", 5, 12.38, 2.04)
  ).toDF("zone", "number_of_dropoffs", "average_total_fare", "average_trip_distance")

  val sample_dropoffsPerBoroughStatsDF = Seq(
    ("Manhattan", 8, 15.36, 3.04),
    ("Queens", 2, 24.13, 6.54)
  ).toDF("borough", "number_of_dropoffs", "average_total_fare", "average_trip_distance")

  val sample_pickDropStatsDFWithRank = Seq(
    ("Lenox Hill West", "Upper East Side South", 3, 1)
  ).toDF("pickup_zone", "dropoff_zone", "number_of_dropoffs", "rank")


  val sample_pick_drop_taxi_data = ReadWriteUtils.readCSV(spark, pickDropTaxiEncoderSchema, "src/test/resources/ny_taxi_data_large/ny_taxi_pickdrop_data/pick-drop.csv").as[PickDropTaxiData]
  val sample_pickup_taxi_data = ReadWriteUtils.readCSV(spark, pickupTaxiEncoderSchema, "src/test/resources/ny_taxi_data_large/ny_taxi_pickup_data/pickup.csv").as[PickupTaxiData]
  val sample_dropoff_taxi_data = ReadWriteUtils.readCSV(spark, dropOffTaxiEncoderSchema, "src/test/resources/ny_taxi_data_large/ny_taxi_dropoff_data/dropoff.csv").as[DropOffTaxiData]
  val sample_filtered_taxidata = ReadWriteUtils.readCSV(spark, taxiEncoderSchema, "src/test/resources/ny_taxi_data_large/ny_taxi_filtered_data").as[TaxiData]

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
    val expected = sample_filtered_taxidata.collectAsList()
    calculated.asScala must beEqualTo(expected.asScala)
  }

  def calculateFareTest: MatchResult[mutable.Buffer[Row]] = {
    import spark.implicits._
    logger.info("Test Case 5: Should calculate the average total fare for each trip_distance rounded to 0 decimal places and the number of trips correctly")
    val outputPath = "src/test/resources/ny_taxi_insights/insight1"
    val result = Main.calculateFare(sample_pick_drop_taxi_data, outputPath).run(spark).filter($"trip_distance" >20 )
    val calculated = result.collectAsList()
    val expected = sample_taxiFareData.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

  def calculateNumberOfPickUpsPerZoneTest: MatchResult[mutable.Buffer[Row]] = {
    println("Test Case 6: Should calculate number of pickups per zone correctly in the sample data")
    val outputPath = "src/test/resources/ny_taxi_insights/insight2"
    val result = Main.calculateNumberOfPickUpsPerZone(sample_pickup_taxi_data.limit(6), outputPath).run(spark)
    val calculated = result.collectAsList()
    val expected = sample_numberOfPickUpsPerZoneDF.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

  def calculateNumberOfPickUpsPerBoroughTest: MatchResult[mutable.Buffer[Row]] = {
    println("Test Case 7: Should calculate number of pickups per borough correctly for the sample data")
    val outputPath = "src/test/resources/ny_taxi_insights/insight3"
    val result = Main.calculateNumberOfPickUpsPerBorough(sample_pickup_taxi_data.limit(10), outputPath).run(spark)
    val calculated = result.collectAsList()
    val expected = sample_numberOfPickUpsPerBoroughDF.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

  def calculateDropOffsPerZoneStatsTest: MatchResult[mutable.Buffer[Row]] = {
    println("Test Case 8: Should calculate drop off statistics for Alphabet City correctly for the sample data")
    val outputPath = "src/test/resources/ny_taxi_insights/insight4"
    val result = Main.calculateDropOffsPerZoneStats(sample_dropoff_taxi_data.filter($"dropoff_zone" === "Alphabet City"), outputPath).run(spark)
    val calculated = result.collectAsList()
    val expected = sample_dropoffsPerZoneStatsDF.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

  def calculateDropOffsPerBoroughStatsTest: MatchResult[mutable.Buffer[Row]] = {
    println("Test Case 9: Should calculate drop off statistics per borough correctly for the sample data")
    val outputPath = "src/test/resources/ny_taxi_insights/insight5"
    val result = Main.calculateDropOffsPerBoroughStats(sample_dropoff_taxi_data.limit(10), outputPath).run(spark)
    val calculated = result.collectAsList()
    val expected = sample_dropoffsPerBoroughStatsDF.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

  def calculateTopDropoffZonePerPickupZoneTest: MatchResult[mutable.Buffer[Row]] = {
    println("Test Case 10: Should calculate top drop off zones for Lenox Hill West for the sample data")
    val outputPath = "src/test/resources/ny_taxi_insights/insight6"
    val result = Main.calculateTopDropoffZonePerPickupZone(sample_pick_drop_taxi_data.filter($"pickup_zone" === "Lenox Hill West"), outputPath).run(spark)
    val calculated = result.limit(1).collectAsList()
    val expected = sample_pickDropStatsDFWithRank.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

}

