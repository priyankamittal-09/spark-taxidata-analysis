# Challenge: Taxi Data Analysis                                       

## Tools/Resources Used

- scala 2.12.12                               (Coding Language)
- Apache Spark 3.0.1                          (Processing Framework)
- Intellij Idea Ultimate Edition   2019.3.1   (Development Tool)
- SBT 1.4.6                                   (Build Tool)

## Project Structure
```
├── spark-movie-rating-analysis
    ├── build.sbt
    ├── project
    │   ├── build.properties
    ├── README.md
│   ├── src
│   │   ├── main
│   │   │   ├── resources
│   │   │   │   ├── log4j.properties
│   │   │   │   ├── ny_taxi_insights
│   │   │   │   └── ny_taxi_test_data
│   │   │   │       ├── ny_taxi
│   │   │   │       ├── ny_taxi_rtbf
│   │   │   │       │   └── rtbf_taxies.csv
│   │   │   │       └── ny_zones
│   │   │   │           └── ny_taxi_zones.csv
│   │   │   └── scala
│   │   │       └── com
│   │   │           └── data
│   │   │               ├── GenerateTestData.scala
│   │   │               ├── Main.scala
│   │   │               ├── model
│   │   │               │   ├── DropOffTaxiData.scala
│   │   │               │   ├── PickDropTaxiData.scala
│   │   │               │   ├── PickupTaxiData.scala
│   │   │               │   └── TaxiData.scala
│   │   │               ├── package.scala
│   │   │               └── utils
│   │   │                   └── ReadWriteUtils.scala
│   │   └── test
│   │       ├── resources
│   │       │   ├── ny_taxi_data_large
│   │       │   │   ├── ny_taxi
│   │       │   │   │   ├── part-00000-3ba72e4d-fd58-441f-98a7-652d60398eee-c000.snappy.parquet
│   │       │   │   │   └── _SUCCESS
│   │       │   │   ├── ny_taxi_dropoff_data
│   │       │   │   │   ├── part-00000-5f7463ef-68ab-4a1f-8246-b00ac1eeb144-c000.csv
│   │       │   │   │   └── _SUCCESS
│   │       │   │   ├── ny_taxi_filtered_data
│   │       │   │   │   ├── part-00000-711b6964-61a0-469c-8cb8-8c1c1086dfcf-c000.csv
│   │       │   │   │   └── _SUCCESS
│   │       │   │   ├── ny_taxi_pickdrop_data
│   │       │   │   │   ├── pick-drop.csv
│   │       │   │   │   └── _SUCCESS
│   │       │   │   ├── ny_taxi_pickup_data
│   │       │   │   │   ├── part-00000-c7df7414-8c83-464e-9bf4-22224129d3f3-c000.csv
│   │       │   │   │   └── _SUCCESS
│   │       │   │   ├── ny_taxi_rtbf
│   │       │   │   │   └── rtbf_taxies.csv
│   │       │   │   └── ny_zones
│   │       │   │       └── ny_taxi_zones.csv
│   │       │   ├── ny_taxi_data_small
│   │       │   │   ├── ny_taxi
│   │       │   │   │   ├── part-00000-87a44e9b-d0a6-4c1d-a78d-c374d6108c4c-c000.snappy.parquet
│   │       │   │   │   └── _SUCCESS
│   │       │   │   ├── ny_taxi_rtbf
│   │       │   │   │   └── rtbf_taxies.csv
│   │       │   │   └── ny_zones
│   │       │   │       └── ny_taxi_zones.csv
│   │       │   └── ny_taxi_insights
│   │       │       └── insight1
│   │       │           ├── part-00000-3d94c4a6-db8f-4fc9-9e61-1e6a21a2f87b-c000.csv
│   │       │           └── _SUCCESS
│   │       └── scala
│   │           └── com
│   │               └── data
│   │                   └── MainSpec.scala
```

## Steps to run using Docker

### 1. Install docker 
https://docs.docker.com/install/

### 2. Pull the docker image from docker hub
docker pull priyankamittal09/scala-spark-3.0.1

### 3. This step is to download the docker file and start the docker container.
docker run -it --rm priyankamittal09/scala-spark-3.0.1:latest /bin/sh

### 4. (optional step) Script to pull spark project from the git repository to run SBT tests. You should by default be on the /app directory level.
sh test-script.sh 

### 5. Main script to run the jar using spark-submit
sh start-script.sh

### 6. To exit docker container
exit





