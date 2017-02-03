import sys
import re
f = open(sys.argv[1], 'r')
f2 = open(sys.argv[2], 'r')
num = sys.argv[3]
X=""
X2=""
Y="\["
while True :
        i = f.readline()
        X = X + i
        if i == "":
                break
while True :
        i = f2.readline()
        X2 = X2 + i
        if i == "":
                break				
for i in range(int(num)-1):
        Y = Y + "\d+, "
Y = Y + "\d+\]"
R =  re.findall(Y, X)
R2 = re.findall(Y, X2)

myset = set(R)
myset2 = set(R2)
#print len(myset)

mylist = list(myset)
mylist2 = list(myset2)

#print len(mylist)
for i in range(len(mylist)):
	mylist[i] = mylist[i][1:-1]
	sort_list = mylist[i].split(", ")
	sort_list.sort()
	mylist[i] = " ".join(str(x) for x in sort_list)

for i in range(len(mylist2)):
	mylist2[i] = mylist2[i][1:-1]
	sort_list = mylist2[i].split(", ")
	sort_list.sort()
	mylist2[i] = " ".join(str(x) for x in sort_list)	

frequent_set = set(mylist)
frequent_set2 = set(mylist2)
print len(frequent_set&frequent_set2)
