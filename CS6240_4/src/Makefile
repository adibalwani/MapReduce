#!bin/bash
# Author: Adib Alwani

######################### Compile ##############################

compile: compile_pseudo

download_lib:
	mkdir lib
	wget -O lib/hadoop-annotations-2.6.3.jar https://www.dropbox.com/s/hf1zyx5u0vxrmbw/hadoop-annotations-2.6.3.jar?dl=0
	wget -O lib/hadoop-common-2.6.3.jar https://www.dropbox.com/s/wkz233whuboo6bz/hadoop-common-2.6.3.jar?dl=0
	wget -O lib/hadoop-mapreduce-client-core-2.6.3.jar https://www.dropbox.com/s/xveq1qefjde2hnk/hadoop-mapreduce-client-core-2.6.3.jar?dl=0

compile_pseudo: download_lib
	javac -cp "lib/*" LinearRegression.java
	jar cvf job.jar *.class

########################## Run ##################################

run: clean pseudo

pseudo: setup_hdfs compile run_pseudo hstop

run_pseudo:
	hadoop LinearRegression /user/adib/input/all output
	hadoop fs -get output
	cat output/part* > finaloutput

########################## HDFS Setup ###########################

setup_hdfs: hstop format hstart dir_adib put_data

format:
	rm -rf /tmp/hadoop*/*
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop:
	mr-jobhistory-daemon.sh stop historyserver 
	stop-yarn.sh
	stop-dfs.sh

dir_adib:
	hadoop fs -mkdir -p /user/adib
	hadoop fs -mkdir -p /user/adib/input

put_data:
	hadoop fs -put all /user/adib/input/

########################### Clean ###############################

clean:
	rm -rf output *.class *.jar finaloutput lib

clean_submission: clean
	rm -rf all
