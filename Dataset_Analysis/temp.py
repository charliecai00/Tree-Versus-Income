import csv

dic = {}

with open("health.csv") as f:
    csv_reader = csv.reader(f, delimiter=',')
    
    for line in csv_reader:
        print(line)
        temp_dic = {}
        for i in range (1, len(line)):
            if "Good" in line[i]:
                temp_dic["Good"] = int(line[i].strip("Good:"))
        
            if "Fair" in line[i]:
                temp_dic["Fair"] = int(line[i].strip("Fair:"))
                
            if "Poor" in line[i]:
                temp_dic["Poor"] = int(line[i].strip("Poor:"))
        dic[line[0]] = temp_dic
writef = open("health_copy.csv", 'w')

print(dic)
for i in dic:
    writef.write("{},{},{},{}\n".format(i, dic[i]["Good"], dic[i]["Fair"], dic[i]["Poor"]))