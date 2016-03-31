# Author: Adib Alwani

# Args Check
if [ $# -ne 3 ]; then
	echo usage: $0 "<Program Name> <Input Bucket> <Output Bucket>"
	exit 1
fi

# Compile
make compile
python chunk.py $2
python transferDNS.py
sh copy.sh
sh startJob.sh
sh outputS3.sh
python filemerge.py
