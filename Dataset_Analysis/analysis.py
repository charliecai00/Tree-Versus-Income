from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("IncomeTree").getOrCreate()

# Read the income.csv file into a PySpark DataFrame
df1 = spark.read.csv('income.csv', header=False, inferSchema=True)
df1 = df1.toDF('Zip', 'MedianIncome')

# Read the tree_dbh.csv file into a PySpark DataFrame
df2 = spark.read.csv('tree_dbh.csv', header=False, inferSchema=True)
df2 = df2.toDF('Zip', 'TreeDBH')

# Read the health_summary.csv file into a PySpark DataFrame
df3 = spark.read.csv('health_summary.csv', header=False, inferSchema=True)
df3 = df3.toDF('zip', 'Good', 'Fair', 'Poor')

df1.show()
df2.show()
df3.show()