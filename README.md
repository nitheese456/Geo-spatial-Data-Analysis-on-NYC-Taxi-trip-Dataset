# Project-1-Group-11

## Steps to execute the code

1) Pull the latest docker image from the dockerhub - [rajkumaar23/group11-project1-phase1-bonus:v0](https://hub.docker.com/repository/docker/rajkumaar23/group11-project1-phase1-bonus)
2) Run the image using the command - `docker run -dit <docker-image-name>`
3) Enter the docker container using the command - `docker exec -it <docker-container-hash> /bin/bash`
4) In the "/root/cse511" folder execute the command - `sbt assembly`. The JAR file will be created in the location - "target/scala-2.11/CSE511-assembly-0.1.0.jar"
5) After the creation of the above JAR file, run the following command to get the ouptut in the "result/output" folder - `spark-submit target/scala-2.11/CSE511-assembly-0.1.0.jar result/output rangequery src/resources/arealm10000.csv -93.63173,33.0183,-93.359203,33.219456 rangejoinquery src/resources/arealm10000.csv src/resources/zcta10000.csv distancequery src/resources/arealm10000.csv -88.331492,32.324142 1 distancejoinquery src/resources/arealm10000.csv src/resources/arealm10000.csv 0.1`.
6) Verify the output in "result/output*/part-0001*.csv" files.
