import os
os.system("hadoop fs -rmr output")
#os.system("/usr/local/apache-maven-3.1.0/bin/mvn -DskipTests install -e")
os.system("bin/mahout fpg -i /user/root/input/fpg_webdocs_1692000.dat -o patterns -k 1000 -method mapreduce -regex '[\ ]' -s 500000")
os.system("hadoop fs -cat output/result_1>>ess_NPATTERN_500000")

os.system("hadoop fs -rmr output")
os.system("bin/mahout fpg -i /user/root/input/fpg_webdocs_1692000.dat -o patterns -k 1000 -method mapreduce -regex '[\ ]' -s 450000")
os.system("hadoop fs -cat output/result_1>>ess_NPATTERN_450000")

os.system("hadoop fs -rmr output")
os.system("bin/mahout fpg -i /user/root/input/fpg_webdocs_1692000.dat -o patterns -k 1000 -method mapreduce -regex '[\ ]' -s 400000")
os.system("hadoop fs -cat output/result_1>>ess_NPATTERN_400000")

os.system("hadoop fs -rmr output")
os.system("bin/mahout fpg -i /user/root/input/fpg_webdocs_1692000.dat -o patterns -k 1000 -method mapreduce -regex '[\ ]' -s 389160")
os.system("hadoop fs -cat output/result_1>>ess_NPATTERN_389160")

os.system("hadoop fs -rmr output")
os.system("bin/mahout fpg -i /user/root/input/fpg_webdocs_1692000.dat -o patterns -k 1000 -method mapreduce -regex '[\ ]' -s 338400")
os.system("hadoop fs -cat output/result_1>>ess_NPATTERN_338400")

os.system("hadoop fs -rmr output")
os.system("bin/mahout fpg -i /user/root/input/fpg_webdocs_1692000.dat -o patterns -k 1000 -method mapreduce -regex '[\ ]' -s 300000")
os.system("hadoop fs -cat output/result_1>>ess_NPATTERN_300000")

os.system("hadoop fs -rmr output")
os.system("bin/mahout fpg -i /user/root/input/fpg_webdocs_1692000.dat -o patterns -k 1000 -method mapreduce -regex '[\ ]' -s 250000")
os.system("hadoop fs -cat output/result_1>>ess_NPATTERN_250000")

os.system("hadoop fs -rmr output")
os.system("bin/mahout fpg -i /user/root/input/fpg_webdocs_1692000.dat -o patterns -k 1000 -method mapreduce -regex '[\ ]' -s 200000")
os.system("hadoop fs -cat output/result_1>>ess_NPATTERN_200000")
