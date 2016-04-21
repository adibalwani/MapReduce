# @author : Rusikesh Badami

import sys
from subprocess import call
import commands

f = 1
with open('original-dns', 'r+') as fd:
	for i, line in enumerate(fd):
		nodes="ec2-user@" + str(line)[:-1] + ":~/"
		if i != 0:
			call(["scp", "-i", "ec2-key.pem", "s3data"+str(f),nodes])
			f = f + 1
		call(["scp", "-i", "ec2-key.pem", "-r", "classFiles/.", nodes])
fd.close()
			


