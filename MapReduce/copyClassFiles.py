# @author : Rushikesh Badami

import sys
import os
from subprocess import call
import commands
from itertools import islice

'''
Below block of code is used to transfer class files of our implemented hadoop to each worker node and master 
'''
prefixed = [filename for filename in os.listdir('.') if filename.startswith("s3data")]  # Command used to check the number of instances  
print prefixed
num_of_files=len(prefixed) 	# Counter used to store the number of files to be transferred
print num_of_files

'''
Below block of code is used to transfer a list of single file to each worker node with all classfiles of our hadoop implementation 
'''
if(num_of_files==1):
	with open("instance-dns") as dns:
    		head = list(islice(dns, 2))			# List master and worker node and store in list
		node="ec2-user@"+str(head[1]).replace("\n","")+":~/"  		# Store user address of each worker node in this variable
		print node
		call(["scp", "-i", "ec2-key.pem", "s3data",node])	# Transfer list of files to be download from s3 to worker node.
		dns.close()
	with open('original-dns', 'r+') as fd:
		for i, line in enumerate(fd):
			nodes="ec2-user@" + str(line)[:-1] + ":~/"
			call(["scp", "-i", "ec2-key.pem", "-r", "classFiles/.", nodes])		# Transfer classFiles of our hadoop implementation to worker node and master
		fd.close()
'''
Below block of code is used to transfer a list of files to be downloaded by each worker node from s3 to process data .This block also transfer class files of our implementation
to master and each worker node.
'''
else:
	f = 1											# Counter to transfer the correct list of input files to be downloaded from s3 to each worker node
	with open('original-dns', 'r+') as fd:
		for i, line in enumerate(fd):
			nodes="ec2-user@" + str(line)[:-1] + ":~/"
			if i != 0:
				call(["scp", "-i", "ec2-key.pem", "s3data"+str(f),nodes])	# Transfer list of files to be download from s3 to worker nodes
				f = f + 1
			call(["scp", "-i", "ec2-key.pem", "-r", "classFiles/.", nodes])		# Transfer classFiles of our hadoop implementation to worker nodes and master
	fd.close()
			
			


