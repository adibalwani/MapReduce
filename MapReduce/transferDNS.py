# @author : Rusikesh Badami

import sys
from subprocess import call
import subprocess
import time
import commands

lines = 0
path=[]


#Cleaning master and worker nodes.Creating required folders at master and worker nodes
with open('original-dns','r+') as fm:
	for line in fm:
		print line
		nodes="ec2-user@"+str(line)[:-1]
		if nodes != "ec2-user@null":
			#print nodes
			#print lines
			call(["ssh-keygen", "-R",str(line)[:-1]])
			flag=1
			while flag !=0:
				try:
					time.sleep(2)
					if lines==0:
						remove=subprocess.check_call(["ssh","-i","ec2-key2.pem","-o","StrictHostKeyChecking=no",nodes,"rm -rf","~./aws"])
						print "remove status",remove
						flag=0
					else:
						remove=subprocess.check_call(["ssh","-i","ec2-key2.pem","-o","StrictHostKeyChecking=no",nodes,"rm -rf","~./aws","input","output","partition"])
						print "remove status",remove
						flag=0
						
				except:
					flag=1
			flag2=1
			while flag2 !=0:
				try:
					if lines==0:
						time.sleep(2)
						var=subprocess.check_call(["ssh","-i","ec2-key2.pem","-o","StrictHostKeyChecking=no",nodes,"mkdir","~/.aws"])
						flag2=0
					else:
						var=subprocess.check_call(["ssh","-i","ec2-key2.pem","-o","StrictHostKeyChecking=no",nodes,"mkdir","~/.aws","input","output","partition"])
					flag2=0
				except:
					flag2=1
			call(["scp","-i","ec2-key2.pem","credentials",nodes+":~/.aws"])
			src=nodes+":~/"
			path.append(src)
			time.sleep(5)
			lines=lines+1					
					

#transfer dns file to master
with open('original-dns', 'r+') as fd:
	f1 = open('instance-dns', 'w+')
	for i, line in enumerate(fd):
		src=line[:-1]
		if(src != "null"):
			if var == i:
				f1.write(line[:-1] + "master" + "\n")
			else:
				f1.write(line[:-1] + "\n")	
	var += 1
	f1.close()
	call(["scp", "-i", "ec2-key2.pem", "instance-dns","download.py", "Makefile", "hadoop.jar", path[0]])

#transfer required files to worker nodes.
j=1
with open('instance-dns','r+') as fs:
	for line in fs:
		call(["scp", "-i", "ec2-key2.pem", "s3data"+str(j),"download.py", "Makefile", "hadoop.jar", path[j]])
		j=j+1
	fs.close()
			


