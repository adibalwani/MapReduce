# @author : Rushikesh Badami

import sys
from subprocess import call
import subprocess
import time
import commands

lines = 0				# Counter used to count number of lines in file
path=[] 				# List used to store dns address of each node 

'''
This block of code is used to remove aws credentials,input,output folder from each node(i.e EC2 instance ).
'''
with open('original-dns','r+') as fm:
	for line in fm:
		print line
		nodes="ec2-user@"+str(line)[:-1]		# This variable is used to store DNS address of each node. 
		if nodes != "ec2-user@null":
			call(["ssh-keygen", "-R",str(line)[:-1]])	# Remove ec2-instances from known hosts.If already present 
			flag = 1					# This flag is set to one if there were no errors while performing below operations.
			while flag != 0:
				try:
					time.sleep(2)
					if lines == 0:
						# This command is used to delete aws folder from the master using ssh login.
						remove = subprocess.check_call(["ssh","-i","ec2-key.pem","-o","StrictHostKeyChecking=no",nodes,"rm -rf","~/.aws"])     
						print "remove status",remove
						flag = 0
					else:
						# This command is used to delete aws input and output folder from worker nodes using ssh login.
						remove = subprocess.check_call(["ssh","-i","ec2-key.pem","-o","StrictHostKeyChecking=no",nodes,"rm -rf","~/.aws","input","output"])
						print "remove status",remove
						flag = 0
						
				except:
					flag = 1
			flag2 = 1	# This flag is used to check if required folders are created on ec2 instances without any errors.
'''
Below block of code is used to create aws input and output folder on each ec2-instance i.e Master,Worker Nodes.
'''			
			while flag2 != 0:
				try:
					if lines == 0:
						time.sleep(2)
						# This command is used to create a aws credentials folder on master ec2-instance.
						var = subprocess.check_call(["ssh","-i","ec2-key.pem","-o","StrictHostKeyChecking=no",nodes,"mkdir","~/.aws"])
						flag2 = 0
					else:
						# This command is used to create input,output,aws credentials folder on worker nodes (ec2-instances) .
						var = subprocess.check_call(["ssh","-i","ec2-key.pem","-o","StrictHostKeyChecking=no",nodes,"mkdir","~/.aws","input","output"])
						flag2 = 0
				except:
					flag2 = 1
			
			call(["scp","-i","ec2-key.pem","credentials",nodes+":~/.aws"]) 		# Copy our aws credentials to each worker node.
			src = nodes + ":~/"							# Variable used to store user name of each ec2-instance.
			path.append(src)							# Append user name of each ec2-instance to a list.
			time.sleep(5)
			lines = lines+1

			
'''
Below block of code is used to transfer makefile,our hadoop.jar,download.py file,list of worker nodes and our private key to each ec2 instance i.e Worker node and Master.
'''

for j in range(0,lines):
	with open('original-dns', 'r+') as fd:
		f1 = open('instance-dns', 'w+') 					# Write data with address of each node to instance-dns file
		for i, line in enumerate(fd):
			if var == i:
				f1.write(line[:-1] + " current" + "\n")			
			else:
				f1.write(line[:-1] + "\n")	
		var += 1
		f1.close()
		call(["scp", "-i", "ec2-key.pem", "instance-dns", "download.py", "Makefile", "hadoop.jar", path[j]])	# Command used to transfer required files to each ec2-instance
		time.sleep(2)
	fd.close()
			


