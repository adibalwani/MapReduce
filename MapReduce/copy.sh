#!bin/bash
# Author: Rachit Puri and Rushikesh Badami

var=1
master=0
while read id
do
	echo $id
	while [ $var != "0" ] && [ $master != "0" ]; do
			ssh -i ec2-key.pem -o StrictHostKeyChecking=no ec2-user@$id "python download.py" &
			var=0
			echo $var
	done
	var=1
	master=1
done < original-dns
wait
