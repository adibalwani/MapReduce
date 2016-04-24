# @author : Rushikesh Badami

import sys
import os
from subprocess import call
import commands
from itertools import islice


prefixed = [filename for filename in os.listdir('.') if filename.startswith("s3data")]
print prefixed
num_of_files=len(prefixed)
print num_of_files
if(num_of_files==1):
	with open("instance-dns") as dns:
    		head = list(islice(dns, 2))
		node="ec2-user@"+str(head[1]).replace("\n","")+":~/"
		print node
		call(["scp", "-i", "ec2-key.pem", "s3data",node])
		dns.close()
else:
	f = 1
	with open('original-dns', 'r+') as fd:
		for i, line in enumerate(fd):
			nodes="ec2-user@" + str(line)[:-1] + ":~/"
			if i != 0:
				call(["scp", "-i", "ec2-key.pem", "s3data"+str(f),nodes])
				f = f + 1
			call(["scp", "-i", "ec2-key.pem", "-r", "classFiles/.", nodes])
	fd.close()
			


