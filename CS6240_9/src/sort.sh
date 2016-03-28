#!bin/bash
# Author: Adib Alwani

# SCP data into EC2 Instances
while read dns
do
	scp -i ec2-key.pem instance-dns ec2-user@$dns:~
done < instance-dns
