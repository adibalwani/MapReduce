# Author: Rachit Puri and Rushikesh Badami

import sys
from subprocess import call
import math

bucketname = sys.argv[1] 						# Name of AWS bucket where all the input files are present

instances = 0.0								# Number of ec2 instances running for the program
check_for_file=[]							# Check if the data in aws s3 bucket is a directory or a single file
check_for_file=bucketname.split("/")
extract_last=check_for_file[(len(check_for_file))-1]			# Extract the name of file or directory



'''
This block of code is used to count the number of running ec2 instances spawned by script file start-cluster.sh.
This block of code reads the instance-dns file and counts the running ec2 instances.
'''

with open("instance-dns", "r") as fd:
	for line in fd:
		instances += 1
	instances = instances - 1
	print instances
fd.close()

'''
This block code is used to list the number of files from aws s3 .Sort the file from 
'''
if ".txt" not in extract_last:
	fs3 = open("s3data", "w+")
	fd = open("listfilesFilter", "w+")
	f = open("listfiles", "w+")
	call(["aws", "s3", "ls", bucketname + "/"], stdout=f) 	      # List all files from s3 and redirect output to file named Listfiles
	call(["sort", "-nk3", "listfiles"], stdout=fd)		      # Sort files according to size
	call(["awk", "NR>1", "listfilesFilter"], stdout=fs3)	      # This command removes first lines from s3 data which is not needed
	fs3.close()
	fd.close()

	listoffiles = []					     # List used to store all files listed by s3

'''
Store data from files to List
'''
	with open('s3data', 'r+') as fm:
		for line in fm:
			words = line.split(" ")
			listoffiles.append(bucketname + "/" + words[len(words)-1])

	f8 = open('sorteds3data','w+')	
	j = len(listoffiles)					# Calculate the length of list and store in counter
	i = 0
	k = j
	var = len(listoffiles)

'''
Block of code used to split the input data into equal chunks of files .These chunks will be downloaded by each worker node for performing
Map Reduce operation.
'''
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

	nofiles = float(len(listoffiles))		# Counter used to count and store number of files present input data.
	split = int(nofiles/instances)			# Counter used to store number of files to be assigned to each worker node.
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
	
