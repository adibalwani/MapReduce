import sys
import os
from subprocess import call
import re

for f in os.listdir('.'):
	if re.match('s3data', f):
		filename = f
		print filename

with open(filename, 'r') as fd:
	for line in fd:
		call(["aws", "s3", "cp", line[:-1], "./input"])

fd.close()