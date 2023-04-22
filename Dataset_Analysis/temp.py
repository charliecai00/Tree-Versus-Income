import csv

dic = {}

with open("/mnt/c/users/charl/OneDrive/documents/tree-versus-income/dataset_analysis/health.csv") as f:
    csv_reader = csv.reader(f, delimiter=',')
    
    for line in csv_reader:
        temp_dic = {}
        temp_dic[line[0]] = {}
        for i in range (1, len(line)):
            if "Good" in line[i]:
                temp_dic[line[0]]["Good"] = int(line[i].strip("Good:"))
        
            if "Fair" in line[i]:
                temp_dic[line[0]]["Fair"] = int(line[i].strip("Fair:"))
                
            if "Poor" in line[i]:
                temp_dic[line[0]]["Poor"] = int(line[i].strip("Poor:"))
                
writef = open("/mnt/c/users/charl/OneDrive/documents/tree-versus-income/dataset_analysis/health_copy.csv", 'w')

print(dic)
for i in dic:
    writef.write("{},{},{},{}\n".format(i, dic[i]["Good"], dic[i]["Fair"], dic[i]["Poor"]))