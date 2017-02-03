import sys
import re
f = open(sys.argv[1], 'r')
num = sys.argv[2]
X=""
Y="\["
while True :
        i = f.readline()
        X = X + i
        if i == "":
                break
for i in range(int(num)-1):
        Y = Y + "\d+, "
Y = Y + "\d+\]"
R =  re.findall(Y, X)

myset = set(R)

print len(myset)

mylist = list(myset)


#for i in range(len(mylist)):
#	mylist[i] = mylist[i][1:-1]
#	sort_list = mylist[i].split(", ")
#	sort_list.sort()
#	for j in range(len(sort_list)):
#		if len(sort_list)>2:
#			print sort_list[j]+" ",
#	if len(sort_list)>2:
#		print "\n",

