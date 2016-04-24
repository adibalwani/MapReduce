#!bin/bash
# Author: Rachit Puri and Rushikesh Badami


: '
This script is used to run download.py on each worker node by establishing ssh connection with worker node.
The download.py script downloads the chunk of data that is to be processed by each worker node on that worker node in input folder
'

var=1				# Counter to check if the node is master or worker node
master=0			# Counter whoose value is the master node

# Block of code used to get user name and download required files from s3 on that instance.
while read id
do
	echo $id
	while [ $var != "0" ] && [ $master != "0" ]; do
			ssh -i ec2-key.pem -o StrictHostKeyChecking=no ec2-user@$id "python download.py" &  # Shell command to open ssh connection with ec2-instances and download data from s3
			var=0
			echo $var
	done
	var=1
	master=1
done < original-dns
wait
