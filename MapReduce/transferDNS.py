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
			flag = 1
			while flag != 0:
				try:
					time.sleep(2)
					if lines == 0:
						remove = subprocess.check_call(["ssh","-i","ec2-key.pem","-o","StrictHostKeyChecking=no",nodes,"rm -rf","~/.aws"])
						print "remove status",remove
						flag = 0
					else:
						remove = subprocess.check_call(["ssh","-i","ec2-key.pem","-o","StrictHostKeyChecking=no",nodes,"rm -rf","~/.aws","input","output"])
						print "remove status",remove
						flag = 0
						
				except:
					flag = 1
			flag2 = 1
			
			while flag2 != 0:
				try:
					if lines == 0:
						time.sleep(2)
						var = subprocess.check_call(["ssh","-i","ec2-key.pem","-o","StrictHostKeyChecking=no",nodes,"mkdir","~/.aws"])
						flag2 = 0
					else:
						var = subprocess.check_call(["ssh","-i","ec2-key.pem","-o","StrictHostKeyChecking=no",nodes,"mkdir","~/.aws","input","output"])
						flag2 = 0
				except:
					flag2 = 1
			# copy aws credentials to all the nodes
			call(["scp","-i","ec2-key.pem","credentials",nodes+":~/.aws"])
			src = nodes + ":~/"
			path.append(src)
			time.sleep(5)
			lines = lines+1
			

f = 1
for j in range(0,lines):
	with open('original-dns', 'r+') as fd:
		f1 = open('instance-dns', 'w+')
		for i, line in enumerate(fd):
			if var == i:
				f1.write(line[:-1] + " current" + "\n")
			else:
				f1.write(line[:-1] + "\n")	
		var += 1
		f1.close()
		call(["scp", "-i", "ec2-key.pem", "instance-dns", "download.py", "Makefile", "hadoop.jar", path[j]])
		f = f + 1
		time.sleep(2)
	fd.close()
			


