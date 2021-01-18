package com.data

import com.data.utils.ReadWriteUtils
import com.data.model.{DropOffTaxiData, PickDropTaxiData, PickupTaxiData, TaxiData}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions.{count, _}
import org.apache.spark.sql.types.{DateType, LongType, StructType}

object Main extends App{

  implicit val logger = Logger.getLogger(this.getClass)

  var taxiDataPath = ""
  var zonesDataPath = ""
  var rtbfDataPath = ""
  var insight1Path = ""
  var insight2Path = ""
  var insight3Path = ""
  var insight4Path = ""
  var insight5Path = ""
  var insight6Path = ""

  if (args.length == 9) {
    taxiDataPath = args(0)
    zonesDataPath = args(1)
    rtbfDataPath = args(2)
    insight1Path = args(3)
    insight2Path = args(4)
    insight3Path = args(5)
    insight4Path = args(6)
    insight5Path = args(7)
    insight6Path = args(8)

  } else {
    logger.error("Invalid number of arguments. Enter Valid Arguments : taxiDataPath zonesDataPath rtbfDataPath insight1Path insight2Path insight3Path insight4Path insight5Path insight6Path")
    System.exit(2)
  }

  val log_level = "info"


  (for {
    ny_taxi_original <- readTaxiData(taxiDataPath)
    ny_taxi_rtbf <- readRightToBeForgottenData(rtbfDataPath)
    ny_zones <- readZonesData(zonesDataPath)
    ny_taxi_filtered <- filteredTaxiData(ny_taxi_original, ny_taxi_rtbf)
    taxi_pickup_data <- processPickupDetails(ny_taxi_filtered, ny_zones)
    taxi_dropoff_data <- processDropOffDetails(ny_taxi_filtered, ny_zones)
    taxi_pickup_dropoff_data <- processPickupDropOffDetails(taxi_pickup_data, ny_zones)
    taxiFareDataDF <- calculateFare(taxi_pickup_dropoff_data, insight1Path)
    maxTaxiFareDataDF <- calculateFareMaxTripDistance(taxiFareDataDF)
    numberOfPickUpsPerZoneDF <- calculateNumberOfPickUpsPerZone(taxi_pickup_data, insight2Path)
    numberOfPickUpsPerBoroughDF <- calculateNumberOfPickUpsPerBorough(taxi_pickup_data, insight3Path)
    dropoffsPerZoneStatsDF <- calculateDropOffsPerZoneStats(taxi_dropoff_data, insight4Path)
    dropoffsPerBoroughStatsDF <- calculateDropOffsPerBoroughStats(taxi_dropoff_data, insight5Path)
    pickDropStatsDFWithRank <- calculateTopDropoffZonePerPickupZone(taxi_pickup_dropoff_data, insight6Path)
    dailyStatsDF <- calculateAverageNumberOfTripsPerHourOfDay(ny_taxi_filtered)
    _ <- closeSparkSession()
  } yield ()).run(spark)

  import spark.implicits._

  // TODO Read taxi data
  def readTaxiData(path: String)(implicit logger:Logger): SparkSessionReader[Dataset[TaxiData]] = {
    SparkSessionReader { spark =>
      val ny_taxi_original = spark
        .read
        .schema(taxiEncoderSchema)
        .parquet(path)
        .as[TaxiData]
        .filter($"trip_distance">0)

      logger.info("Original Taxi Data")
      ny_taxi_original.printSchema()
      ny_taxi_original.show()
      ny_taxi_original
    }
  }

// TODO Read right to be forgotten data
  def readRightToBeForgottenData(path: String)(implicit logger:Logger): SparkSessionReader[DataFrame] = {
    SparkSessionReader { spark =>
      val ny_taxi_rtbf = ReadWriteUtils
        .readCSV(spark, rtbfSchema, path)
        .toDF("taxi_id")

      logger.info("Right To Be Forgotten Data")
      ny_taxi_rtbf.printSchema()
      ny_taxi_rtbf.show()
      ny_taxi_rtbf
    }
  }

  // TODO Task 6: Load geocoding data ny_taxi_zones into Spark.

  def readZonesData(path: String)(implicit logger:Logger): SparkSessionReader[DataFrame] = {
    SparkSessionReader { spark =>
      val ny_zones = ReadWriteUtils
        .readCSV(spark, zoneSchema, path)
        .toDF("h3_index", "zone", "borough", "location_id")
        .withColumn("h3_index", lower(conv($"h3_index",10,16)))   // h3 index in base 10 (Decimal) format  --> h3 index in base 16 (HEX) format

      logger.info("New York Zones")
      ny_zones.printSchema()
      ny_zones.show()
      ny_zones
    }
  }

  // TODO Task 4: Now we can process the data; filter the ny_taxi data between the dates 2015-01-15 to 2015-02-15 using the tpep_pickup_datetime column.
  // TODO Task 5: Filter right to be forgotten taxi_ids. Remove all rows that have a taxi_id that is in the ny_taxi_rtbf list.

  def filteredTaxiData(ny_taxi_original: Dataset[TaxiData], ny_taxi_rtbf: DataFrame)(implicit logger:Logger): SparkSessionReader[Dataset[TaxiData]] =
    SparkSessionReader { spark =>
      import spark.implicits._
      val ny_taxi_filtered = ny_taxi_original
        .withColumn("pickupDate", $"tpep_pickup_datetime".cast(DateType))
        .filter($"pickupDate".between("2015-01-15", "2015-02-15"))
        .join(ny_taxi_rtbf, Seq("taxi_id"), "left_anti")
      logger.info("Filtered Taxi Data")
      ny_taxi_filtered.printSchema()
      ny_taxi_filtered.show()
      ny_taxi_filtered.as[TaxiData].cache()

    }

  // TODO Task 7: Using the geocoding data (ny_taxi_zones) and the appropriate index column in the ny_taxi data, geocode each pickup location with zone and borough. We would like 2 new columns: pickup_zone and pickup_borough

  def processPickupDetails(ny_taxi_filtered: Dataset[TaxiData], ny_zones: DataFrame)(implicit logger:Logger): SparkSessionReader[Dataset[PickupTaxiData]] =
    SparkSessionReader { spark =>
      import spark.implicits._
      val ny_taxi_pickup_data = ny_taxi_filtered
        .join(ny_zones, ny_taxi_filtered.col("pickup_h3_index") === ny_zones.col("h3_index"), "left_outer")
        .withColumnRenamed("zone", "pickup_zone")
        .withColumnRenamed("borough", "pickup_borough")
        .drop($"location_id")
        .drop($"h3_index")
        .drop($"pickupDate")

      logger.info("Taxi Data with pickup details")
      ny_taxi_pickup_data.printSchema()
      val count = ny_taxi_pickup_data.count()
      logger.debug(s"Count of Taxi pick up records: $count")
      ny_taxi_pickup_data.show()
      ny_taxi_pickup_data.as[PickupTaxiData].cache()
    }

  // TODO Task 8: Using the geocoding data (ny_taxi_zones) and the appropriate index column in the ny_taxi data, geocode each dropoff location with zone and borough. We would like 2 new columns: dropoff_zone and dropoff_borough.

  def processDropOffDetails(ny_taxi_filtered: Dataset[TaxiData], ny_zones: DataFrame)(implicit logger:Logger): SparkSessionReader[Dataset[DropOffTaxiData]] =
    SparkSessionReader { spark =>
      import spark.implicits._
      val ny_taxi_dropoff_data = ny_taxi_filtered
        .join(ny_zones, ny_taxi_filtered.col("dropoff_h3_index") === ny_zones.col("h3_index"), "left_outer")
        .withColumnRenamed("zone", "dropoff_zone")
        .withColumnRenamed("borough", "dropoff_borough")
        .drop($"location_id")
        .drop($"h3_index")
        .drop($"pickupDate")

      logger.info("Taxi Data with drop off details")
      ny_taxi_dropoff_data.printSchema()
      val count = ny_taxi_dropoff_data.count()
      logger.debug(s"Count of Taxi drop records: $count")
      ny_taxi_dropoff_data.show()
      ny_taxi_dropoff_data.as[DropOffTaxiData].cache()
    }

  // TODO Processed Taxi data

  def processPickupDropOffDetails(ny_taxi_pickup_data: Dataset[PickupTaxiData], ny_zones: DataFrame)(implicit logger:Logger): SparkSessionReader[Dataset[PickDropTaxiData]] =
    SparkSessionReader { spark =>
      import spark.implicits._
      val taxi_pickup_dropoff_data = ny_taxi_pickup_data
        .join(ny_zones, ny_taxi_pickup_data.col("dropoff_h3_index") === ny_zones.col("h3_index"), "left_outer")
        .withColumnRenamed("zone", "dropoff_zone")
        .withColumnRenamed("borough", "dropoff_borough")
        .drop($"location_id")
        .drop($"h3_index")
        .drop($"pickupDate")
      logger.info("Taxi Data with pick up & drop off details")
      taxi_pickup_dropoff_data.printSchema()
      val count = taxi_pickup_dropoff_data.count()
      logger.debug(s"Count of Taxi pick-drop records: $count")
      taxi_pickup_dropoff_data.orderBy("trip_distance").show()
      taxi_pickup_dropoff_data.as[PickDropTaxiData].cache()
    }

  // TODO Insight 1: Calculate the average total fare for each trip_distance (trip distance bin size of 1, or rounded to 0 decimal places) and the number of trips.
  //  Order the output by trip_distance.  Write the output as a single csv with headers. The resulting output should have 3 columns: trip_distance, average_total_fare and number_of_trips.
  // TODO Filter rows if any of the columns: pickup_zone, pickup_borough, dropoff_zone and dropoff_borough are null.


  def calculateFare(taxi_pickup_dropoff_data: Dataset[PickDropTaxiData], outputPath: String)(implicit logger:Logger): SparkSessionReader[DataFrame] =
    SparkSessionReader { spark =>
      import spark.implicits._
      val taxiFareDataDF = taxi_pickup_dropoff_data
        .filter($"pickup_zone".isNotNull && $"pickup_borough".isNotNull && $"dropoff_zone".isNotNull && $"dropoff_borough".isNotNull)
        .select("trip_distance", "total_amount")
        .withColumn("trip_distance", round(col("trip_distance"), 0))
        .withColumn("trip_distance", $"trip_distance".cast(LongType))
        .groupBy("trip_distance").agg(
           avg("total_amount").as("average_total_fare"),
           count("trip_distance").as("number_of_trips")
          )
        .withColumn("average_total_fare", round(col("average_total_fare"), 2))
        .orderBy("trip_distance")
      logger.info("Average Taxi Fare Data")
      taxiFareDataDF.printSchema()
      taxiFareDataDF.show()

      ReadWriteUtils.writeCSV(taxiFareDataDF, outputPath)
      taxiFareDataDF
    }

  // TODO Looking at the output of step 9, decide what would be an appropriate upper limit of trip_distance and rerun with this filtered.

  def calculateFareMaxTripDistance(taxiFareDataDF: DataFrame)(implicit logger:Logger): SparkSessionReader[DataFrame] =
    SparkSessionReader { spark =>
      import spark.implicits._
      val max_trip_distance = taxiFareDataDF
        .agg(max("trip_distance").as(""))
        .collect()(0)(0).asInstanceOf[Long]
      logger.debug(s"max_trip_distance: $max_trip_distance")
      val maxTaxiFareDataDF = taxiFareDataDF.filter($"trip_distance" === max_trip_distance)

      logger.info("Average Taxi Fare Data for max trip distance")
      maxTaxiFareDataDF.show()
      maxTaxiFareDataDF
    }

  // TODO Insight 2: Total number of pickups in each zone. Write the output as a single csv with headers. The resulting output should have 2 columns: zone and number_of_pickups.

def calculateNumberOfPickUpsPerZone(ny_taxi_pickup_data: Dataset[PickupTaxiData], outputPath: String): SparkSessionReader[DataFrame] =
  SparkSessionReader { spark =>
    val numberOfPickUpsPerZoneDF = ny_taxi_pickup_data
      .filter($"pickup_zone".isNotNull )
      .groupBy("pickup_zone")
      .count()
      .withColumnRenamed("pickup_zone","zone")
      .withColumnRenamed("count","number_of_pickups")
    logger.info("Number of pickups in each zone")
    numberOfPickUpsPerZoneDF.printSchema()
    numberOfPickUpsPerZoneDF.show(false)

    ReadWriteUtils.writeCSV(numberOfPickUpsPerZoneDF, outputPath)
    numberOfPickUpsPerZoneDF
  }

  // TODO Insight 3: Total number of pickups in each borough. Write the output as a single csv with headers. The resulting output should have 2 columns: borough and number_of_pickups.

  def calculateNumberOfPickUpsPerBorough(ny_taxi_pickup_data: Dataset[PickupTaxiData], outputPath: String): SparkSessionReader[DataFrame] =
    SparkSessionReader { spark =>
      val numberOfPickUpsPerBoroughDF = ny_taxi_pickup_data
        .filter($"pickup_borough".isNotNull )
        .groupBy("pickup_borough")
        .count()
        .withColumnRenamed("count","number_of_pickups")
        .withColumnRenamed("pickup_borough","borough")
      logger.info("Number of pickups in each borough")
      numberOfPickUpsPerBoroughDF.printSchema()
      numberOfPickUpsPerBoroughDF.show(false)

      ReadWriteUtils.writeCSV(numberOfPickUpsPerBoroughDF, outputPath)
      numberOfPickUpsPerBoroughDF
    }


  // TODO Insight 4: Total number of dropoffs, average total cost and average distance in each zone.
  //  Write the output as a single csv with headers. The resulting output should have 4 columns: zone, number_of_dropoffs, average_total_fare and average_trip_distance.

  def calculateDropOffsPerZoneStats(ny_taxi_dropoff_data: Dataset[DropOffTaxiData], outputPath: String): SparkSessionReader[DataFrame] =
    SparkSessionReader { spark =>
      val dropoffsPerZoneStatsDF = ny_taxi_dropoff_data
        .filter($"dropoff_zone".isNotNull )
        .groupBy("dropoff_zone")
        .agg(
          count("trip_distance").as("number_of_dropoffs"),
          avg("total_amount").as("average_total_fare"),
          avg("trip_distance").as("average_trip_distance"))
        .withColumnRenamed("dropoff_zone","zone")
        .withColumn("average_total_fare", round(col("average_total_fare"), 2))
        .withColumn("average_trip_distance", round(col("average_trip_distance"), 2))

      logger.info("Dropoff stats in each zone")
      dropoffsPerZoneStatsDF.printSchema()
      dropoffsPerZoneStatsDF.show(false)

      ReadWriteUtils.writeCSV(dropoffsPerZoneStatsDF, outputPath)
      dropoffsPerZoneStatsDF
    }

  // TODO Insight 5: Total number of dropoffs, average total cost and average distance in each borough.
  // Write the output as a single csv with headers. The resulting output should have 4 columns: borough, number_of_dropoffs, average_total_fare and average_trip_distance.

  def calculateDropOffsPerBoroughStats(ny_taxi_dropoff_data: Dataset[DropOffTaxiData], outputPath: String): SparkSessionReader[DataFrame] =
    SparkSessionReader { spark =>
      val dropoffsPerBoroughStatsDF = ny_taxi_dropoff_data
        .filter($"dropoff_borough".isNotNull )
        .groupBy("dropoff_borough")
        .agg(
          count("trip_distance").as("number_of_dropoffs"),
          avg("total_amount").as("average_total_fare"),
          avg("trip_distance").as("average_trip_distance"))
        .withColumnRenamed("dropoff_borough","borough")
        .withColumn("average_total_fare", round(col("average_total_fare"), 2))
        .withColumn("average_trip_distance", round(col("average_trip_distance"), 2))
      logger.info("Dropoff stats in each borough")
      dropoffsPerBoroughStatsDF.printSchema()
      dropoffsPerBoroughStatsDF.show(false)

      ReadWriteUtils.writeCSV(dropoffsPerBoroughStatsDF, outputPath)
      dropoffsPerBoroughStatsDF
    }

  // TODO Insight 6: For each pickup zone calculate the top 5 dropoff zones ranked by number of trips.
  // Write output as a single csv with headers. The resulting output should have 4 columns: pickup_zone, dropoff_zone, number_of_dropoffs and rank.

  def calculateTopDropoffZonePerPickupZone(taxi_pickup_dropoff_data: Dataset[PickDropTaxiData], outputPath: String): SparkSessionReader[DataFrame] =
    SparkSessionReader { spark =>
      val pickDropStatsDF = taxi_pickup_dropoff_data
        .withColumn("pickup_zone-dropoff_zone",concat(col("pickup_zone"),lit('-'),col("dropoff_zone")))
        .groupBy("pickup_zone-dropoff_zone")
        .count()
        .withColumn("pickup_zone",split(col("pickup_zone-dropoff_zone"),"-").getItem(0))
        .withColumn("dropoff_zone",split(col("pickup_zone-dropoff_zone"),"-").getItem(1))
        .withColumnRenamed("count","number_of_dropoffs")
      logger.debug("Top Dropoff Zone Per Pickup Zone")
      if(log_level == "debug"){
        pickDropStatsDF.printSchema()
        pickDropStatsDF.show(false)
      }

      val partitionWindowByPickUpZone = Window.partitionBy($"pickup_zone").orderBy($"number_of_dropoffs".desc)
      val pickDropStatsDFWithRank = pickDropStatsDF
        .withColumn("rank", rank().over(partitionWindowByPickUpZone))
        .filter($"rank" <= 5)
        .drop("pickup_zone-dropoff_zone")
      logger.info("Top 5 dropoff zones per pickup zone ranked by number of trips")
      pickDropStatsDFWithRank.printSchema()
      pickDropStatsDFWithRank.show(false)

      ReadWriteUtils.writeCSV(pickDropStatsDFWithRank, outputPath)
      pickDropStatsDFWithRank
    }

  // TODO Insight 7: Calculate the number of trips for each date -> pickup hour, (using tpep_pickup_datetime), then calculate the average number of trips by hour of day.
  // The resulting output should have 2 columns: hour_of_day and average_trips.

  def calculateAverageNumberOfTripsPerHourOfDay(ny_taxi_filtered: Dataset[TaxiData]): SparkSessionReader[DataFrame] =
    SparkSessionReader { spark =>
      val numberOfTripsEveryHourDF = ny_taxi_filtered
        .withColumn("hour",hour(col("tpep_pickup_datetime")))
        .groupBy("pickupDate","hour")
        .count()
        .withColumnRenamed("count","number_of_trips")

      logger.debug("Number of trips")
      if(log_level == "debug"){
        numberOfTripsEveryHourDF.printSchema()
        numberOfTripsEveryHourDF.orderBy("pickupDate").show()
      }

      val AverageNumberOfTripsEveryHourDF = numberOfTripsEveryHourDF
        .withColumnRenamed("hour","hour_of_day")
        .groupBy("hour_of_day")
          .agg(avg("number_of_trips").as("average_trips"))
          .orderBy("hour_of_day")
        .withColumn("average_trips", round(col("average_trips"), 4))


      logger.info("Average number of trips each hour of the day")
      AverageNumberOfTripsEveryHourDF.printSchema()
      AverageNumberOfTripsEveryHourDF.show()
      AverageNumberOfTripsEveryHourDF
    }


  def closeSparkSession(): SparkSessionReader[Unit] =
    SparkSessionReader { spark =>
      spark.stop()
    }
}
