# Author: Adib Alwani

#Args Check
if [ $# -ne 5 ]; then
	#echo $1 $2 $3 $4 $5
	echo usage: $0 "<Program Name> <Input Bucket> <Output Bucket>"
	exit 1
fi

make compile
python chunk.py $4
python transferDNS.py
sh copy.sh
sh startJob.sh
sh outputS3.sh
python filemerge.py $5