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
java -cp .:hadoop.jar edu.neu.hadoop.mapreduce.Hadoop $1 $2 $3

#else


# Copy input split to appropriate workers

# Copy program to Workers (Jar)

# Ping Master using telnet on 9999

# Wait for program to end (if needed)

#####################################################################
