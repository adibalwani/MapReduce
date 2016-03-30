import sys
arg=[]
arg=sys.argv
#print(arg)
nofiles=arg[1]
instances=arg[2]
split=int(nofiles)/int(instances)
f1=open('listfiles'+str(1),'w')
partition=split *1
with open('listfiles','r+') as fm:
	i=1
	k=0
	for line in fm:
		
		#print line
		splt=[]
		splt=line.split(" ")
		f1.write (splt[len(splt)-1])
		#print split *i
		#print k==(split*i)
		if(k >= partition and i!=int(instances)):
			print k,split*i
			i=i+1
			partition=split*i
			f1.close()
			print i
			f1=open('listfiles'+str(i),'w')
		elif(i==int(instances) and k==split*(int(instances)-1)):
			print i
			f1.close()
			f1=open('listfiles'+str(i),'w')
		k=k+1	
	f1.close()	
		#print splt[len(splt)-1]
print k		