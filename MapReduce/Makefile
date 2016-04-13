#!/bin/bash
# Author: Adib Alwani

######################### Compile ##############################

find_java_files:
	find src -name "*.java" > javas.txt

find_class_files:
	find bin -name "*.class" > classes.txt

compile: clean find_java_files
	mkdir bin
	javac -d bin @javas.txt

compile_word_count:
	javac -cp .:hadoop.jar -d bin test/WordCount/WordCount.java

make_jar:
	jar cvf hadoop.jar -C bin .

########################## Run ##################################



########################## Clean ################################

clean:
	rm -rf bin classes.txt javas.txt hadoop.jar
