# @author : Rachit Puri and Rusikesh Badami

import sys
from subprocess import call
import time

lines = 0
var = 0
with open('instance-dns', 'r+') as fm:
	for line in fm:
		lines += 1

fm.close()
print (lines)

# line = "ec2-52-37-72-133.us-west-2.compute.amazonaws.com"
# server = "ec2-user@" + str(line)
# print server
# call(["ssh", "-i", "ec2-key.pem", server, "mkdir", "input"])
# path = server + ":~/input"
# call(["scp", "-i", "ec2-key.pem", "del-instance", path])

for j in range(0,lines):
	with open('instance-dns', 'r+') as fd:
		# for line in fd:
		# 	print line
		# 	path = "ec2-user@" + str(line[:-1]) + ":~/input"
		# 	print path 
		f1 = open('del-instance', 'w+')
		for i, line in enumerate(fd):
			server = "ec2-user@" + str(line)[:-1]
			call(["ssh", "-i", "ec2-key.pem", server, "mkdir", "input"])
			path = server + ":~/input"
			print path
			if var == i:
				f1.write(line[:-1] + " current" + "\n")
			else:
				f1.write(line[:-1] + "\n")	
		var += 1
		f1.close()
		print path
		#call(["scp", "-i", "ec2-key.pem", "del-instance", path])
		time.sleep(2)
	fd.close()
