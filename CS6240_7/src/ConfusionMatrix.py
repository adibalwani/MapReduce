# Author : Rachit Puri
f1 = open('solution_final', 'r')
f2 = open('98validate.csv', 'r')

true_true = 0
true_false = 0
false_true = 0
false_false = 0
dict = {}

for line in f2:
	words = line.split(",")
	dict[words[0]] = words[1][:-1]

for line in f1:
	words = line.split(",")
	words[1] = words[1][:-2]
	val = dict.get(words[0], "")
	if words[1] == "TRUE" and val == "TRUE":
		true_true += 1
	elif words[1] == "TRUE" and val == "FALSE":
		true_false += 1
	elif words[1] == "FALSE" and val == "TRUE":
		false_true += 1
	elif words[1] == "FALSE" and val == "FALSE":
		false_false += 1

accuracy = (true_true + false_false) / (float) (true_false + false_true + true_true + false_false) * 100
print ("True_True : " + str(true_true))
print ("True_False : " + str(true_false))
print ("False_True : " + str(false_true))
print ("False_False : " + str(false_false))
print ("Acuracy : " + str(accuracy))
