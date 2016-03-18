f1 = open('test.csv', 'r+')
f2 = open('connectingFlight.csv', 'w')

for line in f1:
	f2.write(line)
	break;

for line in f1:
	words = line.split(",")
	print words[5]
	if "2004-01-01" in line:
		f2.write(line)

f1.close()
f2.close()