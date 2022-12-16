#!/bin/bash
filename="target/test1-0.1.jar"
jobmanager_host=127.0.0.1
jobmanager_port=8081


#echo "Creating new job."
#mvn clean package

echo "Uploading job to jobmanager."
curl -X POST -H "Expect:" -F "jarfile=@$filename" http://$jobmanager_host:$jobmanager_port/jars/upload
