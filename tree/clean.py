import csv

with open("./tree/tree_data.csv") as f:
    csv_reader = csv.reader(f, delimiter=',')
    
    out = open('./tree/output.csv', 'w')
    out.write("tree_id,tree_dbh,health,zipcode,zip_city,boroname\n")
    for line in csv_reader:
        if (line[7] == 'Alive'):
            tree_id = line[1]
            tree_dbh = line[4]
            health = line[8]
            zipcode = line[26]
            zip_city = line[27]
            boroname = line[30]
            keyword = "{},{},{},{},{},{}\n".format(tree_id, tree_dbh, health, zipcode, zip_city, boroname)
            out.write(keyword)