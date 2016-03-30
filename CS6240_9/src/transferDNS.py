# @author : Rachit Puri and Rusikesh Badami

import sys
from subprocess import call
import time

lines = 0
var = 0
path=[]
with open('instance-dns', 'r+') as fm:
	for line in fm:
		server = "ec2-user@" + str(line)[:-1]
		call(["ssh", "-i", "ec2-key.pem", server, "mkdir", "input"])
		src = server + ":~/input"
		path.append(src)
		lines += 1
f=0
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
		call(["scp", "-i", "ec2-key.pem", "del-instance","listfiles"+str(f),path[j]])
		f=f+1
		time.sleep(2)
	fd.close()
