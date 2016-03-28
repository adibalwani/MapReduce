#!bin/bash
# Author: Adib Alwani

# Args check
if [ $# -ne 1 ]; then
	echo usage: $0 "<Number of Instances>"
	exit 1
fi

# Create EC2 Instances
aws ec2 run-instances --image-id ami-c229c0a2 --count $1 --instance-type t2.micro --key-name ec2-key --security-groups ec2-sec-key > instances

# Fetch instanceIds and store
cat instances | jq -r ".Instances[].InstanceId" > instance-ids

# Fetch DNS and store
aws ec2 describe-instances --filters "Name=instance-type,Values=t2.micro" | jq -r ".Reservations[].Instances[].PublicDnsName" | sed '/^$/d' > instance-dns
