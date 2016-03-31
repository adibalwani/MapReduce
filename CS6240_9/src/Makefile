#!/bin/bash
# Author: Adib Alwani

######################### Compile ##############################

compile:
	javac Barrier.java Client.java Node.java Reader.java NetworkManager.java Sampling.java Server.java TempDetails.java TextSocket.java Printer.java
	jar cvf node.jar *.class

########################## Run ##################################
	
run:
	java -cp .:node.jar Node

s3transfer:
	sh outputS3.sh

ec2: run s3transfer

getS3data: 
	python filemerge.py

########################## Clean ################################

clean:
	rm -rf original-dns instance-ids instances *.class *.jar s3data*

clean_submission:
	rm -rf ec2-key.pem
