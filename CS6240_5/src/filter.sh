#!/bin/bash
# Author: Adib Alwani

MISSED_SUM=0
CONNECTING_SUM=0
FILENAME="finaloutput"
OUTPUTFILE="solution_final"
KEY=""

echo -n "" > $OUTPUTFILE

while read line
do
	KEY_LINE="$(echo $line | awk '{print $1 " " $2}')"
	MISSED_SUM_LINE="$(echo $line | awk '{print $4}')"
	CONNECTING_SUM_LINE="$(echo $line | awk '{print $5}')"
	if [ "$KEY" = "$KEY_LINE" ]
	then
		MISSED_SUM=$((MISSED_SUM+MISSED_SUM_LINE))
		CONNECTING_SUM=$((CONNECTING_SUM+CONNECTING_SUM_LINE))
	else
		echo $KEY " " $MISSED_SUM " " $CONNECTING_SUM >> $OUTPUTFILE
		KEY=$KEY_LINE
		MISSED_SUM=0
		CONNECTING_SUM=0
	fi
done < $FILENAME

sort $OUTPUTFILE | sed -i '1d' $OUTPUTFILE
