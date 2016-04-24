#!/bin/bash
# Author: Adib Alwani

######################### Compile ##############################

download_lib:
	rm -rf lib
	mkdir lib
	wget -O lib/weka-3.7.3.jar https://www.dropbox.com/s/cyvffylllu0o6ce/weka-3.7.3.jar?dl=0

find_java_files:
	find src -name "*.java" > javas.txt

find_class_files:
	find bin -name "*.class" > classes.txt

compile: clean find_java_files compile_map_reduce form_jar

compile_map_reduce:
	mkdir bin
	javac -d bin @javas.txt

compile_word_count:
	javac -cp .:hadoop.jar -d . test/WordCount/WordCount.java

compile_assignment_2:
	mkdir classFiles
	javac -cp .:hadoop.jar -d classFiles test/Ass2/ClusterAnalysis.java

compile_assignment_7:
	mkdir classFiles
	donwload_lib
	cp hadoop.jar lib
	javac -cp "lib/*" -d classFiles test/Ass2/ClusterAnalysis.java
	
form_jar:
	jar cvf hadoop.jar -C bin .

########################## Run ##################################

run_word_count: clean compile compile_word_count

run_assignment_2: clean compile compile_assignment_2

run_master:
	java -cp .:hadoop.jar edu.neu.hadoop.mapreduce.main.Master

run_worker:
	java -cp .:hadoop.jar edu.neu.hadoop.mapreduce.main.Worker

########################## Clean ################################

clean:
	rm -rf bin classes.txt javas.txt hadoop.jar *.class partition output fat_jar s3data* listfilesFilter listfiles sorteds3data classFiles
