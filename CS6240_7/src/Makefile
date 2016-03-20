#!bin/bash
# Author: Adib Alwani

######################### Globals ##############################

BUCKET_NAME = adibpurimane1
INPUT_BUCKET_NAME = mrclassvitek

######################### Compile ##############################

download_lib:
	rm -rf lib
	mkdir lib
	wget -O lib/hadoop-annotations-2.6.3.jar https://www.dropbox.com/s/hf1zyx5u0vxrmbw/hadoop-annotations-2.6.3.jar?dl=0
	wget -O lib/hadoop-common-2.6.3.jar https://www.dropbox.com/s/wkz233whuboo6bz/hadoop-common-2.6.3.jar?dl=0
	wget -O lib/hadoop-mapreduce-client-core-2.6.3.jar https://www.dropbox.com/s/xveq1qefjde2hnk/hadoop-mapreduce-client-core-2.6.3.jar?dl=0
	wget -O lib/weka-3.7.3.jar https://www.dropbox.com/s/cyvffylllu0o6ce/weka-3.7.3.jar?dl=0

compile_prediction:
	javac -cp "lib/*" FlightDetail.java FlightHandler.java ModelMapper.java ModelReducer.java Prediction.java TestMapper.java TestReducer.java

form_fat_jar:
	rm -rf fat_jar
	mkdir fat_jar
	unzip lib/weka-3.7.3.jar -d fat_jar
	rm -rf fat_jar/META-INF
	cp *.class fat_jar
	jar cvf job.jar -C fat_jar .

compile: download_lib compile_prediction form_fat_jar

######################### AWS ###################################

configure_s3:
	aws s3 mb s3://$(BUCKET_NAME)
	aws s3 rm s3://$(BUCKET_NAME)/output --recursive
	aws s3 rm s3://$(BUCKET_NAME)/model_output --recursive
	aws s3 cp job.jar s3://$(BUCKET_NAME)/

delete_bucket:
	aws s3 rb s3://$(BUCKET_NAME) --force

create_cluster:
	aws emr create-cluster \
    --name "CLI Test Cluster" \
    --release-label emr-4.3.0 \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge \
    --steps Type=CUSTOM_JAR,Name="CLI Test JAR Step",ActionOnFailure=CONTINUE,Jar=s3://$(BUCKET_NAME)/job.jar,MainClass=Prediction,Args=[s3://$(INPUT_BUCKET_NAME)/a6history,s3://$(BUCKET_NAME)/model_output,s3://$(INPUT_BUCKET_NAME)/a6test,s3://$(BUCKET_NAME)/output] \
    --auto-terminate \
    --log-uri s3://$(BUCKET_NAME)/log \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a\
    --enable-debugging > clusterId.txt

wait_for_completion:
	python checkstatus.py

get_s3_data:
	aws s3 cp s3://$(BUCKET_NAME)/output output --recursive
	cat output/part* > solution_final

########################## Run ##################################

pseudo: clean setup_hdfs compile run_pseudo confusion_matrix hstop

run_pseudo:
	hadoop jar job.jar Prediction /user/adib/input/a6history model_output /user/adib/input/a6test output
	hadoop fs -get output
	cat output/part* > solution_final

confusion_matrix:
	gunzip -c input/a6validate/98validate.csv.gz > 98validate.csv
	python ConfusionMatrix.py > confusion_matrix

emr: compile configure_s3 create_cluster wait_for_completion get_s3_data confusion_matrix delete_bucket

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

put_data:
	hadoop fs -put input /user/adib/

########################### Clean ###############################

clean:
	rm -rf output *.class *.jar finaloutput lib health.json clusterId.txt solution_final result fat_jar confusion_matrix 98validate.csv

clean_submission: clean
	rm -rf input
