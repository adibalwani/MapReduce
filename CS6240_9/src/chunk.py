# Author: Rachit Puri and Rushikesh Badami

import sys
from subprocess import call
import math

# print 'Number of arguments:', len(sys.argv), 'arguments.'
# print 'Argument List:', str(sys.argv)

#bucketname = sys.argv[1]
bucketname = "s3://cs6240sp16/climate"
instances = 0.0
nofiles = 0.0
with open("original-dns", "r") as fd:
	for line in fd:
		instances += 1

print instances
fd.close()

fs3 = open("s3data", "w+")

#f = open("listfiles", "r+")
#call(["aws", "s3", "ls", bucketname + "/"], stdout=f)
#f.close()

with open('listfiles', 'r+') as fm:
	for line in fm:
		filename = line.rsplit(' ', 1)
		#print filename[1][:-1]
		if len(filename[1]) > 3:
			fs3.write(bucketname +  "/" +filename[1][:-1] + "\n")
			nofiles += 1
fs3.close()
fm.close()


arg = []
arg = sys.argv
split = math.ceil(float(nofiles/instances))
print nofiles
print split
f1 = open('s3data'+str(1),'w')
partition=split *1
with open('s3data','r+') as fm:
	i=1
	k=1
	for line in fm:
		splt=[]
		splt=line.split(" ")
		f1.write (splt[len(splt)-1])
		if(k >= partition and i!=int(instances)):
			i=i+1
			partition=split*i
			f1.close()
			f1=open('s3data'+str(i),'w')
		elif(i==int(instances) and k==split*(int(instances)-1)):
			f1.close()
			f1=open('s3data'+str(i),'w')
		k=k+1	
	f1.close()	
