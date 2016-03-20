# Author : Rachit Puri
f1 = open('missed_flight', 'r')
f2 = open('04missed.csv', 'r')

correct = 0
incorrect = 0
dict = {}
prevkey = ""

for line in f2:
	words = line.split(",")
	key = words[0] + "," + words[1] + "," + words[2] + "," + words[3] + "," + words[4]
	value = words[5] + "," +words[6][:-1]
	if key == prevkey:
		dict[key].append(value)
	else:
		dict[key] = [value]
	prevkey = key

for line in f1:
	flag = "FALSE"
	words = line.split(",")
	key = words[0] + "," + words[1] + "," + words[2] + "," + words[3] + "," + words[4]
	value = words[5] + "," +words[6][:-2]
	#print value
	val = dict.get(key, "")
	print val
	#print len(val)
	for i, num in enumerate(val):
		print num
		if num == value:
			correct += 1
			flag = "TRUE"
	
	if flag != "TRUE":
		incorrect += 1	

	
#print correct
#print incorrect
accuracy = (correct) / (correct + incorrect) * 100
print ("accuracy is " +accuracy)
