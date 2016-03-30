#!bin/bash
# Author: Adib Alwani, Rachit Puri, Rushikesh Badami, Bhavin Vora
var=1
instance=$0
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
# sleep 4
# aws s3 ls  s3://cs6240sp16/climate/ > listfiles
# echo "$(tail -n +2 listfiles)" > listfiles
# nofiles=$(wc -l listfiles|cut -d ' ' -f1)
# python chunk.py $nofiles $instances
# sleep 3
# python transferDNS.py


