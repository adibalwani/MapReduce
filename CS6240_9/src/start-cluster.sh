# Author: Adib Alwani, Rachit Puri, Rushikesh Badami, Bhavin Vora
var=1
instance=$0
# Args check
if [ $# -ne 1 ]; then
	echo usage: $0 "<Number of Instances>"
	exit 1
fi

# Create EC2 Instances
aws ec2 run-instances --image-id ami-c229c0a2 --count $1 --instance-type m3.medium --key-name ec2-key --security-groups ec2-sec-key > instances

# Fetch instanceIds and store
cat instances | jq -r ".Instances[].InstanceId" > instance-ids

# Fetch DNS and store
aws ec2 describe-instances --filters "Name=instance-type,Values=m3.medium" | jq -r ".Reservations[].Instances[].PublicDnsName" | sed '/^$/d' > original-dns

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

# copy aws credentials to current directory
cp ~/.aws/credentials ./

sleep 6

# python chunk.py
# python transferDNS.py
# sh copy.sh

echo "Set Up Completed"
