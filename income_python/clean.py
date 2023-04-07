import csv

income = {} # Pelham Parkway, 55273.71398
zip = {} # Pelham Parkway, [10461,10467]

with open("./income/med_income.csv") as f:
    csv_reader = csv.reader(f, delimiter=',')
    
    for line in csv_reader:
        if (line[1] == 'All Households' and line[2] == '2015'):
            location = line[0]
            data = line[4]
            income[location] = data
            
with open("./income/med_income_zipcodes.txt", 'r') as f:
    for line in f:
        line = line.strip().split(',')
        location = line[0]
        data = line[1:]
        zip[location] = data


out = open('./income/output.csv', 'w')
out.write("location,zip,data\n")
for i in zip:
    if i in income:
        for j in zip[i]:
            out.write("{},{},{}\n".format(i,j,income[i]))
    else:
        print(i)