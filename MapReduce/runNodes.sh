#!bin/bash
# Author: Rachit Puri and Rushikesh Badami

var=1
master=0
while read id
do
	echo $id
	while [ $var != "0" ]; do
			if [ $master -eq 0 ]
			then
				ssh -i ec2-key.pem -o StrictHostKeyChecking=no ec2-user@$id "make run_master" &
				var=0
				echo $var
			else
				ssh -i ec2-key.pem -o StrictHostKeyChecking=no ec2-user@$id "make run_worker" &
				var=$?
				echo $var
			fi
	done
	var=1
	master=1
done < original-dns
wait
