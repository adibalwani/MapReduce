#!/bin/bash
# Author: Adib Alwani

FILES_TIME=summary/*-flight-time.txt
FILES_DISTANCE=summary/*-distance.txt

echo -n "" > summary-distance
echo -n "" > summary-flight-time

for f in $FILES_TIME
do
	cat $f | grep "Residual standard error" | awk '{printf substr("'"$f"'", 9, 2) "\t" $4}' >> summary-flight-time
	cat $f | grep "^df$flight_time" | awk '{print "\t" $2}' >> summary-flight-time
done

for f in $FILES_DISTANCE
do
	cat $f | grep "Residual standard error" | awk '{printf substr("'"$f"'", 9, 2) "\t" $4}' >> summary-distance
	cat $f | grep "^df$distance_traveled" | awk '{print "\t" $2}' >> summary-distance
done



echo "carrier\_code flight\_time slope\_flight\_time distance slope\_distance" > analysis
join summary-flight-time summary-distance >> analysis

rm -f summary-distance summary-flight-time
