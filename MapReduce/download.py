# Author: Rachit Puri and Rushikesh Badami

import sys
import os
from subprocess import call
import re

for f in os.listdir('.'):
	if re.match('s3data', f):
		filename = f
		print filename
try :
	with open(filename, 'r') as fd:
		for line in fd:
			call(["aws", "s3", "cp", str(line).replace("\n",""), "./input"])
	fd.close()
except:
	print "No Input data"