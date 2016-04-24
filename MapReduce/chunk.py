# Author: Rachit Puri and Rushikesh Badami

import sys
from subprocess import call
import math

bucketname = sys.argv[1]
#bucketname = "s3://puridata/all"
instances = 0.0
check_for_file=[]
check_for_file=bucketname.split("/")
extract_last=check_for_file[(len(check_for_file))-1]
with open("instance-dns", "r") as fd:
	for line in fd:
		instances += 1
	instances = instances - 1
	print instances
fd.close()
if ".txt" not in extract_last:
	fs3 = open("s3data", "w+")
	fd = open("listfilesFilter", "w+")
	f = open("listfiles", "w+")
	call(["aws", "s3", "ls", bucketname + "/"], stdout=f)
	call(["sort", "-nk3", "listfiles"], stdout=fd)
	call(["awk", "NR>1", "listfilesFilter"], stdout=fs3)
	fs3.close()
	fd.close()

	listoffiles = []
	with open('s3data', 'r+') as fm:
		for line in fm:
			words = line.split(" ")
			listoffiles.append(bucketname + "/" + words[len(words)-1])

	f8 = open('sorteds3data','w+')	
	j = len(listoffiles)
	i = 0
	k = j
	var = len(listoffiles)
	while i < var:
		if var%2 == 0:
			if (i==j):
				break
		else:
			if (i == j-1):
				f8.write(listoffiles[i])
				break
	
		f8.write(listoffiles[i])
		f8.write(listoffiles[j-1])
		i = i+1
		j = j-1
	f8.close()

	nofiles = float(len(listoffiles))
	split = int(nofiles/instances)
	print nofiles
	print split
	f1 = open('s3data'+str(1),'w')
	partition = split *1
	with open('sorteds3data','r+') as fm:
		i = 1
		k = 1
		for line in fm:
			f1.write(line)
			if(k >= partition and i!=int(instances)):
				i = i+1
				partition = split*i
				f1.close()
				f1 = open('s3data'+str(i),'w')
			elif (i==int(instances) and k==split*(int(instances)-1)):
				f1.close()
				f1 = open('s3data'+str(i),'w')
			k = k+1	
		f1.close()	
else:
	file_open=open('s3data','w+')
	file_open.write(bucketname)
	file_open.close()
	
