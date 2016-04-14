#!/bin/bash
# Author: Adib Alwani, Rushikesh Badami

# Args check
if [ $# -ne 3 ]; then
	echo usage: $0 "[Program] [input path] [output path]"
	exit 1
fi

# Start Program
make find_class_files
java -cp .:@classes.txt bin/edu/neu/hadoop/mapreduce/Hadoop $1 $2 $3