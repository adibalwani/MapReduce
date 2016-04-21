# Author: Rachit Puri, Rushikesh Badami
var=1
instance=$0
# Args check
if [ $# -ne 1 ]; then
	echo usage: $0 "<Number of Instances>"
	exit 1
fi

# Create EC2 Instances
aws ec2 run-instances --image-id ami-c229c0a2 --count $1 --instance-type t2.medium --key-name ec2-key2 --security-groups ec2-sec-key > instances

# Fetch instanceIds and store
cat instances | jq -r ".Instances[].InstanceId" > instance-ids

# Fetch DNS and store
aws ec2 describe-instances --filters "Name=instance-type,Values=t2.medium" | jq -r ".Reservations[].Instances[].PublicDnsName" | sed '/^$/d' > original-dns

while read id
do
	while [ $var -eq 1 ]
	do
		if [ $(aws ec2 describe-instance-status --instance-id $id | jq -r ".InstanceStatuses[]" | wc -l) -gt $var ]
		then
			if [ $(aws ec2 describe-instance-status --instance-id $id | jq -r ".InstanceStatuses[].InstanceState.Name") = "running" ]
			then
				echo "instance is running"
				break
			fi
		else
			echo "waiting for instance to start"
		fi
	done
done < instance-ids

# Copy aws credentials to current directory (IDK why this needed?)
cp ~/.aws/credentials ./

# Compile Worker, Master Jar

# Copy AWS credentials, DNSList, Makefile to all nodes

# Copy Master Jar to master

# Copy Worker Jar to worker

# Run make file i.e start threads for all nodes

echo "Set Up Completed"
#########################################################################################

#sleep 6 #(IDK why this is needed?)

# python chunk.py

# Creating Hadoop Jar to upload on ec2 instances.
#find src -name "*.java" > javas.txt
#rm -rf bin hadoop.jar
#mkdir bin
#javac -d bin @javas.txt
#sleep 20
#jar cvf hadoop.jar -C bin .


# python transferDNS.py
# sh copy.sh
# User setup Complete.
