# Author: Rachit Puri and Rushikesh Badami
import os
import sys

BUCKET_NAME = sys.argv[2]

listoffile = []
for file in os.listdir("output/"):
   	listoffile.append(file)

listoffile.sort(reverse=True)
f1 = open('final_output','w+')

for file in listoffile:
	print file
	with  open("output/"+file, 'r+') as fm:
		for line in fm:
			f1.write(line)
f1.close()

aws s3 cp final_output s3://$BUCKET_NAME/output --recursive
	
