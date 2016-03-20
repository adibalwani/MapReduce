#!/bin/bash
# Author: Adib Alwani

######################### Compile ##############################

download_lib:
	rm -rf lib
	mkdir lib
	wget -O lib/hadoop-annotations-2.6.3.jar https://www.dropbox.com/s/hf1zyx5u0vxrmbw/hadoop-annotations-2.6.3.jar?dl=0
	wget -O lib/hadoop-common-2.6.3.jar https://www.dropbox.com/s/wkz233whuboo6bz/hadoop-common-2.6.3.jar?dl=0
	wget -O lib/hadoop-mapreduce-client-core-2.6.3.jar https://www.dropbox.com/s/xveq1qefjde2hnk/hadoop-mapreduce-client-core-2.6.3.jar?dl=0

compile: download_lib
	javac -cp "lib/*" MissedFlight.java
	jar cvf job.jar *.class

######################### AWS ###################################

configure_s3:
	aws s3 mb s3://adibpuri777
	aws s3 rm s3://adibpuri777/output --recursive
	aws s3 cp all s3://adibpuri777/input/ --recursive
	aws s3 cp job.jar s3://adibpuri777/

delete_bucket:
	aws s3 rb s3://adibpuri777 --force

create_cluster:
	aws emr create-cluster \
    --name "CLI Test Cluster" \
    --release-label emr-4.3.0 \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge \
--steps Type=CUSTOM_JAR,Name="CLI Test JAR Step",ActionOnFailure=CONTINUE,Jar=s3://adibpuri777/job.jar,MainClass=MissedFlight,Args=[s3://adibpuri777/input,s3://adibpuri777/output] \
    --auto-terminate \
    --log-uri s3://adibpuri777/log \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a\
    --enable-debugging > clusterId.txt

wait_for_completion:
	python checkstatus.py

get_s3_data:
	aws s3 cp s3://adibpuri777/output result --recursive
	cat result/part* > finaloutput

########################## Run ##################################

run: clean pseudo

pseudo: setup_hdfs compile run_pseudo filter hstop

run_pseudo:
	hadoop MissedFlight /user/adib/all output
	hadoop fs -get output
	cat output/part* > finaloutput

emr: compile configure_s3 create_cluster wait_for_completion get_s3_data delete_bucket filter

filter:
	sh filter.sh

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
	rm -rf output *.class *.jar finaloutput lib health.json clusterId.txt solution_final result

clean_submission: clean
	rm -rf all
