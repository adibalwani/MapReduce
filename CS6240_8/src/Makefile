#!bin/bash
# Author: Adib Alwani

######################### Globals ##############################

BUCKET_NAME = adibpurimane
INPUT_BUCKET_NAME = mrclassvitek

######################### Compile ##############################

download_lib:
	rm -rf lib
	mkdir lib
	wget -O lib/hadoop-annotations-2.6.3.jar https://www.dropbox.com/s/hf1zyx5u0vxrmbw/hadoop-annotations-2.6.3.jar?dl=0
	wget -O lib/hadoop-common-2.6.3.jar https://www.dropbox.com/s/wkz233whuboo6bz/hadoop-common-2.6.3.jar?dl=0
	wget -O lib/hadoop-mapreduce-client-core-2.6.3.jar https://www.dropbox.com/s/xveq1qefjde2hnk/hadoop-mapreduce-client-core-2.6.3.jar?dl=0
	wget -O lib/weka-3.7.3.jar https://www.dropbox.com/s/cyvffylllu0o6ce/weka-3.7.3.jar?dl=0

compile_routing:
	javac -cp "lib/*" FlightDetail.java FlightHandler.java ModelMapper.java ModelReducer.java Routing.java ConnectionMapper.java ConnectionReducer.java TestMapper.java TestReducer.java

form_fat_jar:
	rm -rf fat_jar
	mkdir fat_jar
	unzip lib/weka-3.7.3.jar -d fat_jar
	rm -rf fat_jar/META-INF
	cp *.class fat_jar
	jar cvf job.jar -C fat_jar .

compile: download_lib compile_routing form_fat_jar

######################### AWS ###################################

configure_s3:
	aws s3 mb s3://$(BUCKET_NAME)
	aws s3 rm s3://$(BUCKET_NAME)/output --recursive
	aws s3 rm s3://$(BUCKET_NAME)/model_output --recursive
	aws s3 rm s3://$(BUCKET_NAME)/missed_flight --recursive
	aws s3 cp job.jar s3://$(BUCKET_NAME)/

delete_bucket:
	aws s3 rb s3://$(BUCKET_NAME) --force

create_cluster:
	aws emr create-cluster \
    --name "CLI Test Cluster" \
    --release-label emr-4.3.0 \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m3.xlarge \
                      InstanceGroupType=CORE,InstanceCount=2,InstanceType=m3.xlarge \
    --steps Type=CUSTOM_JAR,Name="CLI Test JAR Step",ActionOnFailure=CONTINUE,Jar=s3://$(BUCKET_NAME)/job.jar,MainClass=Routing,Args=[s3://$(INPUT_BUCKET_NAME)/a7history,s3://$(BUCKET_NAME)/model_output,s3://$(INPUT_BUCKET_NAME)/a7test,s3://$(BUCKET_NAME)/missed_flight,s3://$(INPUT_BUCKET_NAME)/07request,s3://$(BUCKET_NAME)/output] \
    --auto-terminate \
    --log-uri s3://$(BUCKET_NAME)/log \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a\
    --enable-debugging > clusterId.txt

wait_for_completion:
	python checkstatus.py

get_s3_data:
	aws s3 cp s3://$(BUCKET_NAME)/output output --recursive
	aws s3 cp s3://$(BUCKET_NAME)/missed_flight missed_flight --recursive
	cat output/part* > output_final
	cat missed_flight/part* > missed_final

########################## Run ##################################

pseudo: clean setup_hdfs compile run_pseudo confusion_matrix hstop

run_pseudo:
	hadoop jar job.jar Routing /user/adib/input/a7history model_output /user/adib/input/a7test missed_flight /user/adib/input/a7request output
	hadoop fs -get output
	hadoop fs -get missed_flight
	cat output/part* > output_final
	cat missed_flight/part* > missed_final

test: compile_routing form_fat_jar test1 run_pseudo fetch
test1:
	hadoop fs -rm -r -f output
	hadoop fs -rm -r -f missed_flight
	rm -rf missed_flight
	rm -rf output
	rm -rf 1_2004
fetch:
	hadoop fs -get missed_flight
	hadoop fs -get 1_2004
	hadoop fs -get output

confusion_matrix:
	gunzip -c input/a7validate/04missed.csv.gz > 04missed.csv
	python confusionMatrixRouting.py > confusion_matrix

emr: compile configure_s3 create_cluster wait_for_completion get_s3_data confusion_matrix delete_bucket

######################### HDFS Setup ###########################

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
	rm -rf output *.class *.jar lib health.json clusterId.txt missed_flight fat_jar confusion_matrix 04missed.csv output_final missed_final

clean_submission: clean
	rm -rf input
