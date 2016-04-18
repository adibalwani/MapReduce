# @author : Rachit Puri and Rusikesh Badami

import sys
from subprocess import call
import subprocess
import time
import commands

lines = 0
path=[]					

with open('original-dns', 'r+') as fm:
	for line in fm:
		server = "ec2-user@" + str(line)[:-1]
		call(["ssh-keygen", "-R", str(line)[:-1]])
		flag = 1
		while flag != 0:
			try:
				time.sleep(2)
				remove = subprocess.check_call(["ssh", "-i", "ec2-key.pem", "-o", "StrictHostKeyChecking=no", server, "rm -rf", "input", "output", "~/.aws"])
				print "remove status", remove
				flag = 0
			except:
				flag = 1

		flag2 = 1
		while flag2 != 0:
			try:
				time.sleep(2)
				var = subprocess.check_call(["ssh", "-i", "ec2-key.pem", "-o", "StrictHostKeyChecking=no", server, "mkdir", "input", "output", "~/.aws"])
				print "put status", var
				flag2 = 0
			except:
				flag2 = 1
		call(["scp", "-i", "ec2-key.pem", "credentials", server + ":~/.aws/"])
		src = server + ":~/"
		path.append(src)
		lines += 1

f=1
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
		call(["scp", "-i", "ec2-key.pem", "instance-dns","s3data"+str(f), "download.py", "Makefile", "hadoop.jar", path[j]])
		f=f+1
		time.sleep(2)
	fd.close()

