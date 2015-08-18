import random
import threading

f = open("sample.txt", "w");

def printCounter():
	threading.Timer(5.0, printCounter).start()
	print "counter: %.9d, (%.2f percent)" %(counter, (float(counter)/float(myRange)*100))

# 10000000: 140MB
myRange = 80000000
counter = 0

printCounter()

for i in range(1,myRange):
	text = "id%.6d,%d,%d\n" %(random.randrange(1,500000), random.randrange(1,10), random.randrange(1,10))
	f.write(text)
	counter = i

f.close()