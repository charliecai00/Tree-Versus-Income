import csv

tree_dbh_dict = {}
health_dict = {}

with open("./tree/output.csv") as f:
    csv_reader = csv.reader(f, delimiter=',')
    
    for line in csv_reader:
        tree_dbh = line[1]
        health = line[2]
        zipcode = line[3]

        if zipcode in tree_dbh_dict:
            tree_dbh_dict[zipcode].append(tree_dbh)
        else:
            tree_dbh_dict[zipcode] = [tree_dbh]
            
        if zipcode in health_dict:
            health_dict[zipcode].append(health)
        else:
            health_dict[zipcode] = [health]
       
tree_dbh_summary = open("./tree/tree_dbh_summary.csv", 'w')
health_summary = open("./tree/health_summary.csv", 'w')
 
for i in tree_dbh_dict:
    temp = ','.join(tree_dbh_dict[i])
    tree_dbh_summary.write("{},{}\n".format(i,temp))

for i in health_dict:
    temp = ','.join(health_dict[i])
    health_summary.write("{},{}\n".format(i,temp))
            
            
del tree_dbh_dict['zipcode']
del health_dict['zipcode']

    
tree_dbh_summary2 = open("./tree/tree_dbh_summary2.csv", 'w')
health_summary2 = open("./tree/health_summary2.csv", 'w')

for i in tree_dbh_dict:
    sum = 0
    for j in tree_dbh_dict[i]:
        sum += int(j)
    avg = sum / len(tree_dbh_dict[i])
    tree_dbh_summary2.write("{},{}\n".format(i,avg))

for i in health_dict:
    count = {}
    for j in health_dict[i]:
        if j in count:
            count[j] += 1
        else:
            count[j] = 1
    health_summary2.write("{},{}\n".format(i,count))