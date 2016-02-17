#!/bin/bash
# Author: Adib Alwani

MISSED_SUM=0
CONNECTING_SUM=0
KEY=""

echo -n "" > solution_final

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
		echo $KEY " " $MISSED_SUM " " $CONNECTING_SUM >> solution_final
		KEY=$KEY_LINE
		MISSED_SUM=0
		CONNECTING_SUM=0
	fi
done < final_backup

sort solution_final | sed -i '1d' solution_final
