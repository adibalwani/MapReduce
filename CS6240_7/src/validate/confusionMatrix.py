import re

f1 = open('part-r-00000', 'r')
f2 = open('98validate.csv', 'r')

true_true = 0
true_false = 0
false_true = 0
false_false = 0

for line in f1:
	line = re.sub('\s+',' ',line).strip()
	words = line.split(",")
	for test in f2:
		test = re.sub('\s+',' ',test).strip()
		validateWords = test.split(",")
		if words[0] in test:
			if words[1] == "TRUE" and validateWords[1] == "TRUE":
				true_true += 1
				break
			elif words[1] == "TRUE" and validateWords[1] == "FALSE":
				true_false += 1
				break
			elif words[1] == "FALSE" and validateWords[1] == "TRUE":
				false_true += 1
				break
			elif words[1] == "FALSE" and validateWords[1] == "FALSE":
				false_false += 1
				break
	f2.seek(0, 0)

print ("True_True : " +str(true_true))
print ("True_False : " +str(true_false))
print ("False_True : " +str(false_true))
print ("False_False : " +str(false_false))
