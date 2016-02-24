#!/bin/bash
# Author: Adib Alwani

echo "Carrier_Code\tYear\tConnection\tMissedConnection" > solution_final
awk '{a[$1$2]+=$5;}END{for(i in a)print i", "a[i];}' finaloutput > temp1
awk '{a[$1$2]+=$4;}END{for(i in a)print i", "a[i];}' finaloutput > temp2
join temp1 temp2 > solution_final
awk '{print $1 "\t" $2 "\t" $3 "\t" $3*100/$2}' solution_final > temp3
sed -i '1s/^/Carrier_Code\tYear\tConnection\tMissedConnection\tPercentage\n/' temp3
mv temp3 solution_final
rm -rf temp1 temp2 temp3
