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

compile: find_java_files compile_map_reduce form_jar

compile_map_reduce:
	mkdir bin
	javac -d bin @javas.txt
	rm -rf javas.txt

compile_word_count:
	rm -rf tmp
	mkdir tmp
	javac -cp .:hadoop.jar -d tmp test/WordCount/WordCount.java
	jar cvf WordCount.jar -C tmp .
	rm -rf tmp

compile_word_median:
	rm -rf tmp
	mkdir tmp
	javac -cp .:hadoop.jar -d tmp test/WordMedian/WordMedian.java
	jar cvf WordMedian.jar -C tmp .
	rm -rf tmp

compile_assignment_2:
	rm -rf tmp
	mkdir tmp
	javac -cp .:hadoop.jar -d tmp test/Ass2/ClusterAnalysis.java
	jar cvf ClusterAnalysis.jar -C tmp .
	rm -rf tmp

compile_assignment_7: download_lib
	rm -rf tmp
	mkdir tmp
	cp hadoop.jar lib
	javac -cp "lib/*" -d tmp test/Ass7/Prediction/*.java
	javac -cp "lib/*" -d . test/Ass7/Prediction/*.java
	#jar cvf Prediction.jar -C tmp .
	
form_fat_jar: download_lib
	rm -rf tmp
	mkdir tmp
	unzip lib/weka-3.7.3.jar -d tmp
	rm -rf tmp/META-INF
	cp hadoop.jar lib
	javac -cp "lib/*" -d tmp test/Ass7/Prediction/*.java
	jar cvf Prediction.jar -C tmp .

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
	rm -rf bin classes.txt javas.txt hadoop.jar *.class partition output fat_jar s3data* listfilesFilter listfiles sorteds3data classFiles instances instance-ids instance-dns credentials original-dns job.jar

clean_submission:
	rm -rf all alice.txt
