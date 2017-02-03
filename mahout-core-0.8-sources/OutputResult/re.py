import sys
import re
f = open(sys.argv[1], 'r')
X=""
Y="\["
while True :
        i = f.readline()
        X = X + i
        if i == "":
                break
for i in range(1):
        Y = Y + "\d+, "
Y = Y + "\d+\]"
R =  re.findall(Y, X)


f2 = open(sys.argv[2], 'r')
f_count = 0
t_count = 0
count = 0
while True :
	i = f2.readline()
	if i == "":
       		break
	count = count + 1
        j = i.split('.')[1].split('\n')[0]
	if j in R:
		t_count = t_count + 1
	else:
        	f_count = f_count + 1
L = len(R)/2
error = (float(L)-float(count))/float(L)
print ""
print "number of real : "+str(L)
print "number of esstimation : "+str(count)
print "error : "+str(error)
print "right in esstimation : "+str(t_count)
print "false in esstimation : "+str(f_count)
print "number of lost : "+str(L-t_count)
print "lost rate : "+str((float(L)-float(t_count))/float(L))
print ""
