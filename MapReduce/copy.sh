#!bin/bash
# Author: Rachit Puri and Rushikesh Badami

var=1
while read id
do
	echo $id
	while [ $var != "0" ]; do
			ssh -i ec2-key.pem -o StrictHostKeyChecking=no ec2-user@$id "python download.py" &
			var=$?
			echo $var
	done
	var=1
done < original-dns
wait
