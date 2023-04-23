from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("tree_vs_income").getOrCreate()


# Read the income.csv file into a PySpark DataFrame
df1 = spark.read.csv('income.csv', header=False, inferSchema=True)
df1 = df1.toDF('Zip', 'MedianIncome')

# Read the tree_dbh.csv file into a PySpark DataFrame
df2 = spark.read.csv('dbh.csv', header=False, inferSchema=True)
df2 = df2.toDF('Zip', 'TreeDBH')

# Read the health_summary.csv file into a PySpark DataFrame
df3 = spark.read.csv('health.csv', header=False, inferSchema=True)
df3 = df3.toDF('Zip', 'Good', 'Fair', 'Poor')


# Final join
df = df1.join(df2, on='Zip', how='outer')
df = df.join(df3, on='Zip', how='outer')
# Drop rows with null values
final = df.dropna()

final.coalesce(1).write.format("csv").save("output")