import os
listoffile=[]
for file in os.listdir("output/"):
   	listoffile.append(file)
#print listoffile 
listoffile.sort()
f1=open('final-output','w+')
#for file in listoffile:
#	print file

for file in listoffile:
	print file
	with  open("output/"+file, 'r+') as fm:
		for line in fm:
			f1.write(line)
f1.close()

	
