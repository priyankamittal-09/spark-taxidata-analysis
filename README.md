# Challenge: Shopping-Basket                                       

## Tools/Resources Used

- scala 2.12.12                               (Coding Language)
- Apache Spark 3.0.1                          (Processing Framework)
- Intellij Idea Ultimate Edition   2019.3.1   (Development Tool)
- SBT 1.4.6                                   (Build Tool)


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





