# Author: Rachit Puri and Rushikesh Badami

import sys
import os
from subprocess import call
import re

'''
Below block of code is used to read the s3data file on each node and download required files from s3 for data processing.
'''
for f in os.listdir('.'):
	if re.match('s3data', f):
		filename = f  # Get the name of s3data file which contains list of files to be downloaded from s3 for a single worker node
		print filename
try :
	with open(filename, 'r') as fd:
		for line in fd:
			call(["aws", "s3", "cp", str(line).replace("\n",""), "./input"])  # Below command is used to copy the equal chunk of data from s3 for each worker node and store in input folder 
	fd.close()
except:
	print "No Input data"
