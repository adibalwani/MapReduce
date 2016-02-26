# Author: Adib Alwani and Rachit Puri

################################### run locally ######################################

run: clean
	sbt run
	cat out/part* > finaloutput
	sh filter.sh

# create_cluster:
# 	aws emr create-cluster \
#     --name "CLI Test Cluster" \
#     --release-label emr-4.3.0 \
#     --applications Name=Spark \
#     --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium \
#                       InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium \
# --steps Type=CUSTOM_JAR,Name="CLI Test JAR Step",ActionOnFailure=TERMINATE_CLUSTER,Jar=s3://spark6/MissedFlight.jar,MainClass=MissedFlight,Args=[s3://spark6/input,s3://spark6/output] \
#     --auto-terminate \
#     --log-uri s3://spark6/log \
#     --service-role EMR_DefaultRole \
#     --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-west-2a\
#     --enable-debugging

################################## Cluster #########################################

create_cluster:
	aws emr create-cluster \
	--name "Sample Spark Cluster" \
	--release-label emr-4.1.0 \
	--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium \
	--use-default-roles \
	--applications Name=Spark \
	--steps file://./step.json \
	--log-uri s3://spark6/log \
	--auto-terminate

clean:
	rm -rf project target out finaloutput

clean_submission: clean
	rm -rf all
