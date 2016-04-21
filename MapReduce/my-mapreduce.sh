#!/bin/bash
# Author: Adib Alwani, Rushikesh Badami

# Args check
if [ $# -ne 3 ]; then
	echo usage: $0 "[Program] [input path] [output path]"
	exit 1
fi

# Start Program
# if Pseudo
# Compile Hadoop here (makefile)
#java -cp .:hadoop.jar edu.neu.hadoop.mapreduce.main.Hadoop $1 $2 $3

#else

# split the data equally and create files containing path of files to be downloaded by each worker node
python chunk.py $2

python copyClassFiles.py

# Tell each node to download files from S3
sh copy.sh

# Wait for program to end (if needed)
java -cp .:hadoop.jar edu.neu.hadoop.mapreduce.main.Client $1 $2 $3

#####################################################################
