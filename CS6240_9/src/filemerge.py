# Author: Rachit Puri and Rushikesh Badami
import os
import sys
from subprocess import call

BUCKET_NAME = sys.argv[2]

listoffile = []
results = []
for file in os.listdir("output/"):
   	listoffile.append(file)

listoffile = [float(i) for i in listoffile]
listoffile.sort(reverse=True)

results = [str(i) for i in listoffile]
print results
f1 = open('final_output','w+')

for file in results:
	print file
	with  open("output/"+file, 'r+') as fm:
		for line in fm:
			f1.write(line)
f1.close()

call(["aws", "s3", "cp", "final_output", "s3://$BUCKET_NAME/output". "--recursive"])
	
