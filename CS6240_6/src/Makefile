# Author: Adib Alwani

run:
	rm -rf out
	sbt run
	cat out/part* > finaloutput
	sh filter.sh

clean:
	rm -rf project target out

clean_submission: clean
	rm -rf all
