import sys
from subprocess import call

instances = 0
nofiles = 0
with open("instance-dns", "r") as fd:
	for line in fd:
		instances += 1

print instances
fd.close()

fs3 = open("s3data", "w+")

f = open("listfiles", "r+")
call(["aws", "s3", "ls", "s3://cs6240sp16/climate/"], stdout=f)
f.close()

with open('listfiles', 'r+') as fm:
	for line in fm:
		filename = line.rsplit(' ', 1)
		#print filename[1][:-1]
		if len(filename[1]) > 3:
			fs3.write("s3://cs6240sp16/climate/" +filename[1][:-1] + "\n")
			nofiles += 1
fs3.close()
fm.close()


arg = []
arg = sys.argv
split = int(nofiles)/int(instances)
f1 = open('s3data'+str(1),'w')
partition=split *1
with open('s3data','r+') as fm:
	i=1
	k=0
	for line in fm:
		splt=[]
		splt=line.split(" ")
		f1.write (splt[len(splt)-1])
		if(k >= partition and i!=int(instances)):
			print k,split*i
			i=i+1
			partition=split*i
			f1.close()
			print i
			f1=open('s3data'+str(i),'w')
		elif(i==int(instances) and k==split*(int(instances)-1)):
			print i
			f1.close()
			f1=open('s3data'+str(i),'w')
		k=k+1	
	f1.close()	
print k		