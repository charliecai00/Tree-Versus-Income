from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

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
# Evaluate the model's performance
evaluator = RegressionEvaluator(labelCol='TreeDBH', predictionCol='prediction', metricName='rmse')
rmse = evaluator.evaluate(predictions)
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print("# ---------------------------------- DBH ---------------------------------- #")
print('Root mean squared error: %.2f' % rmse)
print('Coefficient of determination (R-squared): %.2f' % r2)
print('Intercept: %.2f' % model.intercept)
print('Slope: %.2f' % model.coefficients[0])
print('R-squared: %.2f' % r2)


# Select the relevant columns
df = final.select(['MedianIncome', 'Good'])
# Split the data into training and testing sets
(training_data, testing_data) = df.randomSplit([0.8, 0.2], seed=42)
# Assemble the features into a vector
assembler = VectorAssembler(inputCols=['MedianIncome'], outputCol='features')
training_data = assembler.transform(training_data)
testing_data = assembler.transform(testing_data)
# Create a linear regression model
lr = LinearRegression(featuresCol='features', labelCol='Good')
# Train the model
model = lr.fit(training_data)
# Make predictions
predictions = model.transform(testing_data)
# Evaluate the model's performance
evaluator = RegressionEvaluator(labelCol='Good', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions)
evaluator = RegressionEvaluator(labelCol='Good', predictionCol='prediction', metricName='r2')
r2 = evaluator.evaluate(predictions)
print("# ---------------------------------- Good ---------------------------------- #")
print('Mean squared error: %.2f' % mse)
print('Coefficient of determination: %.2f' % r2)
print('Intercept: %.2f' % model.intercept)
print('Slope: %.2f' % model.coefficients[0])
print('R-squared: %.2f' % model.summary.r2)


# Select the relevant columns
df = final.select(['MedianIncome', 'Fair'])
# Split the data into training and testing sets
(training_data, testing_data) = df.randomSplit([0.8, 0.2], seed=42)
# Assemble the features into a vector
assembler = VectorAssembler(inputCols=['MedianIncome'], outputCol='features')
training_data = assembler.transform(training_data)
testing_data = assembler.transform(testing_data)
# Create a linear regression model
lr = LinearRegression(featuresCol='features', labelCol='Fair')
lr_model = lr.fit(train_data)
# Make predictions on test data
predictions = lr_model.transform(test_data)
# Evaluate the model's performance
evaluator = RegressionEvaluator(labelCol='Fair', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions)
r2_evaluator = RegressionEvaluator(labelCol='Fair', predictionCol='prediction', metricName='r2')
r2 = r2_evaluator.evaluate(predictions)
print("# ---------------------------------- Fair ---------------------------------- #")
print('Mean squared error: %.2f' % mse)
print('Coefficient of determination: %.2f' % r2)
print('Intercept: %.2f' % lr_model.intercept)
print('Slope: %.2f' % lr_model.coefficients[0])
print('R-squared: %.2f' % lr_model.summary.r2)


# Select the relevant columns
df = final.select(['MedianIncome', 'Poor'])
# Split the data into training and testing sets
(training_data, testing_data) = df.randomSplit([0.8, 0.2], seed=42)
# Assemble the features into a vector
assembler = VectorAssembler(inputCols=['MedianIncome'], outputCol='features')
training_data = assembler.transform(training_data)
testing_data = assembler.transform(testing_data)
# Create a linear regression model
lr = LinearRegression(featuresCol='features', labelCol='Poor')
lr_model = lr.fit(train_data)
# Make predictions on test data
predictions = lr_model.transform(test_data)
# Evaluate the model's performance
evaluator = RegressionEvaluator(labelCol='Fair', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions)
r2_evaluator = RegressionEvaluator(labelCol='Fair', predictionCol='prediction', metricName='r2')
r2 = r2_evaluator.evaluate(predictions)
print("# ---------------------------------- Fair ---------------------------------- #")
print('Mean squared error: %.2f' % mse)
print('Coefficient of determination: %.2f' % r2)
print('Intercept: %.2f' % lr_model.intercept)
print('Slope: %.2f' % lr_model.coefficients[0])
print('R-squared: %.2f' % lr_model.summary.r2)