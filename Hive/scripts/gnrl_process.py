import re
import csv
import time
import sys

a1=sys.argv[1]
a2=sys.argv[2]


pattern1=r"^\d{2}\/\d{2}\/\d{4}$"
pattern2=r"^\d{2}\-\d{2}\-\d{4}$"


st = time.time()
csvfile1=a2
with open(csvfile1, "w") as output:
        with open(a1) as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                        if re.match(pattern1,row[31]):
                                s=row[31]
                                k=s.split('/')
                                #print(k)
                                m=k[2]+"-"+k[0]+"-"+k[1]
                                row[31]=m

                                #print(row)
                                m=""


                        if re.match(pattern2,row[31]):
                                s=row[31]
                                k=s.split('-')
                                #print(k)
                                #print("match2")
                                m=k[2]+"-"+k[1]+"-"+k[0]
                                row[31]=m
                                m=""
                        for q in range(0,len(row)):
                                row[q]=str(row[q])
                                row[q]=row[q].replace(',',' ')

                        writer=csv.writer(output, lineterminator='\n')
                        writer.writerow(row)


print time.time() -st
