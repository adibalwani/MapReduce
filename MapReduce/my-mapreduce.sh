#/bin/bash
# Author: Adib Alwani, Rushikesh Badami

# Args check
if [[ $# -lt 3 ]]; then
	echo usage: $0 "[Program] [input path] [output path]"
	exit 1
fi

#################################### Start Program ##############################
# Check for pseudo mode
if [[ $3 != *"s3"* ]]
then
	rm -rf *.class META*
	unzip $1
	java -cp .:hadoop.jar edu.neu.hadoop.mapreduce.main.Hadoop "$@"
	exit $?
fi

# split the data equally and create files containing path of files to be downloaded by each worker node
python chunk.py $2
rm -rf classFiles
mkdir classFiles
unzip $1 -d classFiles
python copyClassFiles.py

# Tell each node to download files from S3
sh copy.sh

# Wait for program to end (if needed)
java -cp .:hadoop.jar edu.neu.hadoop.mapreduce.main.Client "$@"
