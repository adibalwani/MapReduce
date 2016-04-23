#!/bin/bash
# Author: Adib Alwani

######################### Compile ##############################

find_java_files:
	find src -name "*.java" > javas.txt

find_class_files:
	find bin -name "*.class" > classes.txt

compile: clean find_java_files compile_map_reduce form_jar

compile_map_reduce:
	mkdir bin
	javac -cp "lib/*" -d bin @javas.txt

compile_word_count:
	javac -cp .:hadoop.jar -d . test/WordCount/WordCount.java

compile_assignment_2:
	mkdir classFiles
	javac -cp .:hadoop.jar -d classFiles test/Ass2/ClusterAnalysis.java

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
