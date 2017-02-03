import sys
f = open(sys.argv[1], 'r')
frequent = set()
while True :
	i = f.readline()
	frequent.add(i)
	if i == "":
		break

frequent = list(frequent)
frequent.sort()
i=0
for i in range(len(frequent)):
	print (frequent[i]),
