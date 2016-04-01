#!/bin/bash
# Author: Adib Alwani

######################### Compile ##############################

download_lib:
	rm -rf lib
	mkdir lib
	wget -O lib/org.apache.commons.io.jar https://www.dropbox.com/s/g3mujjhpdrkq8ej/org.apache.commons.io.jar?dl=0

compile_program:
	javac -cp "lib/*" Barrier.java Client.java Node.java Reader.java NetworkManager.java Sampling.java Server.java TempDetails.java TextSocket.java Printer.java

compile: download_lib compile_program form_fat_jar

form_fat_jar:
	rm -rf fat_jar
	mkdir fat_jar
	unzip lib/org.apache.commons.io.jar -d fat_jar
	rm -rf fat_jar/META-INF
	cp *.class fat_jar
	jar cvf node.jar -C fat_jar .

########################## Run ##################################
	
run:
	java -cp .:node.jar Node

s3transfer:
	sh outputS3.sh

ec2: run

getS3data: 
	python filemerge.py

########################## Clean ################################

clean:
	rm -rf *.class *.jar s3data*

clean_submission:
	rm -rf ec2-key.pem
