package com.data.model

import java.sql.Timestamp

case class PickDropTaxiData(
                             vendor_id: Int,
                             tpep_pickup_datetime: Timestamp,
                             tpep_dropoff_datetime: Timestamp,
                             passenger_count: Int,
                             trip_distance: Double,
                             pickup_longitude: Double,
                             pickup_latitude: Double,
                             rate_code_id: Int,
                             store_and_forward: String,
                             dropoff_longitude: Double,
                             dropoff_latitude: Double,
                             payment_type: Int,
                             fare_amount: Double,
                             extra: Double,
                             mta_tax: Double,
                             tip_amount: Double,
                             tolls_amount: Double,
                             improvement_surcharge: Double,
                             total_amount: Double,
                             pickup_h3_index: String,
                             dropoff_h3_index: String,
                             taxi_id: Int,
                             pickup_zone: String,
                             pickup_borough: String,
                             dropoff_zone: String,
                             dropoff_borough: String
                           )