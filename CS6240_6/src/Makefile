# Author: Adib Alwani and Rachit Puri

################################### run locally ######################################

run: clean
	sbt run
	cat out/part* > finaloutput
	sh filter.sh

################################## Cluster #########################################


compile: clean
	sbt package
	mv target/scala-2.10/missed*.jar job.jar
	aws s3 rm s3://spark6/job.jar --recursive
	aws s3 cp job.jar s3://spark6

emr: clean compile create_cluster

create_cluster:
	aws emr create-cluster \
	--name "Sample Spark Cluster" \
	--ami-version 3.11.0 \
	--applications Name=Spark \
	--ec2-attributes KeyName=myKey --instance-type c1.medium --instance-count 3 \
	--steps Type=Spark,Name="Spark Program",ActionOnFailure=TERMINATE_CLUSTER,Args=[--class,MissedFlight,s3://spark6/job.jar,s3://spark6/input,s3://spark6/output] \
	--log-uri s3://spark6/log \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a

clean:
	rm -rf project target out finaloutput job.jar

clean_submission: clean
	rm -rf all
