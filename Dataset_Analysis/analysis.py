from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Create a SparkSession
spark = SparkSession.builder.appName("IncomeTree").getOrCreate()

# Read the income.csv file into a PySpark DataFrame
df1 = spark.read.csv('income.csv', header=False, inferSchema=True)
df1 = df1.toDF('Zip', 'MedianIncome')

# Read the tree_dbh.csv file into a PySpark DataFrame
df2 = spark.read.csv('dbh.csv', header=False, inferSchema=True)
df2 = df2.toDF('Zip', 'TreeDBH')

# Read the health_summary.csv file into a PySpark DataFrame
df3 = spark.read.csv('health.csv', header=False, inferSchema=True)
df3 = df3.toDF('Zip', 'Good', 'Fair', 'Poor')


# Define the schema of the new DataFrame
df4_schema = StructType([
    StructField("Zip", IntegerType(), True),
    StructField("Good", IntegerType(), True),
    StructField("Fair", IntegerType(), True),
    StructField("Poor", IntegerType(), True)
])

# Create an empty DataFrame with the defined schema
df4 = spark.createDataFrame([], df4_schema)

# Loop through each row of df3 and populate df4
for row in df3.rdd.collect():
    good = 0
    fair = 0
    poor = 0
    
    old_good = str(row['Good'])
    old_fair = str(row['Fair'])
    old_poor = str(row['Poor'])
    
    # Parse the value of 'Good'
    if "Good" in old_good:
        good = int(old_good.replace("Good:", ""))
        
    # Parse the value of 'Fair'
    if "Fair" in old_fair:
        fair = int(old_fair.replace("Fair:", ""))
        
    # Parse the value of 'Poor'
    if "Poor" in old_poor:
        poor = int(old_poor.replace("Poor:", ""))
        
    # Append a new row to df4
    df4 = df4.union(
        spark.createDataFrame(
            [(row['Zip'], good, fair, poor)], df4_schema
        )
    )


# Final join
df = df1.join(df2, on='Zip', how='outer')
df = df.join(df4, on='Zip', how='outer')

# Drop rows with null values
final = df.dropna()

# Select columns for input features and target variable
assembler = VectorAssembler(inputCols=["MedianIncome"], outputCol="features")
data = assembler.transform(final).select("TreeDBH", "features")

# Split data into training and testing sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Create a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="TreeDBH")

# Train the model
model = lr.fit(train_data)

# Make predictions on the testing data
predictions = model.transform(test_data)