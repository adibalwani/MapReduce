# @author : Rachit Puri and Rusikesh Badami

import sys
from subprocess import call
import subprocess
import time

lines = 0
var = 0
path=[]
timoeut=0
flag = 0
with open('instance-dns', 'r+') as fm:
	for line in fm:
		server = "ec2-user@" + str(line)[:-1]
		# remote_output = subprocess.check_output(["ssh", "-i", "ec2-key.pem", "-o", "StrictHostKeyChecking=no", server, "mkdir", "input", "~/.aws"], stderr=subprocess.STDOUT)
		# print remote_output
		while flag != 1:
			ssh = subprocess.Popen(["ssh", "-i", "ec2-key.pem", "-o", "StrictHostKeyChecking=no", server, "mkdir", "input", "~/.aws", "ls"], shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			result = ssh.stdout.readlines()
			if result == []:
				error = ssh.stderr.readlines()
				print >>sys.stderr, "ERROR: %s" % error
				if "refused" in error[0]:
					flag = 0
			else:
				flag = 1
				print result
		#call(["ssh", "-i", "ec2-key.pem", "-o", "StrictHostKeyChecking=no", server, "mkdir", "input", "~/.aws"])
		call(["scp", "-i", "ec2-key.pem", "credentials", server + ":~/.aws/"])
		src = server + ":~/"
		path.append(src)
		lines += 1
f=1
for j in range(0,lines):
	with open('instance-dns', 'r+') as fd:
		f1 = open('del-instance', 'w+')
		for i, line in enumerate(fd):
			if var == i:
				f1.write(line[:-1] + " current" + "\n")
			else:
				f1.write(line[:-1] + "\n")	
		var += 1
		f1.close()
		call(["scp", "-i", "ec2-key.pem", "del-instance","s3data"+str(f), "download.py", path[j]])
		f=f+1
		time.sleep(2)
	fd.close()

