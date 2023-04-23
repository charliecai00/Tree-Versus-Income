from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import isnan, sum as pyspark_sum
from pyspark.sql.functions import corr
from pyspark.ml.regression import DecisionTreeRegressor

result = open("result.txt", "w")


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


# use the `corr` function to calculate the correlation
corr_income_dbh = final.select(corr('MedianIncome', 'TreeDBH')).collect()[0][0]
print("Correlation btw income & dbh: {}\n".format(corr_income_dbh))
result.write("Correlation btw income & dbh: {}\n".format(corr_income_dbh))


# convert categorical values to numeric values
final = final.withColumn('Good', final['Good'].cast('integer'))
final = final.withColumn('Fair', final['Fair'].cast('integer'))
final = final.withColumn('Poor', final['Poor'].cast('integer'))

# use the `corr` function to calculate the correlation
corr_income_health = final.select(corr('MedianIncome', 'Good'), corr('MedianIncome', 'Fair'), corr('MedianIncome', 'Poor')).collect()

for row in corr_income_health:
    result.write("\n")
    result.write("Correlation Analysis\n")
    result.write("Correlation btw income & tree with good health: {}\n".format(row['corr(MedianIncome, Good)']))
    result.write("Correlation btw income & tree with fair health: {}\n".format(row['corr(MedianIncome, Fair)']))
    result.write("Correlation btw income & tree with poor health: {}\n".format(row['corr(MedianIncome, Poor)']))
    print("\n")
    print("Correlation Analysis\n")
    print("Correlation btw income & tree with good health: {}\n".format(row['corr(MedianIncome, Good)']))
    print("Correlation btw income & tree with fair health: {}\n".format(row['corr(MedianIncome, Fair)']))
    print("Correlation btw income & tree with poor health: {}\n".format(row['corr(MedianIncome, Poor)']))
        
        
print("\n\n\nRegression Analysis\n\n")


# Select columns for input features and target variable
assembler = VectorAssembler(inputCols=["MedianIncome"], outputCol="features")
data = assembler.transform(final).select("TreeDBH", "features")
# Split data into training and testing sets
train_data_dbh, test_data_dbh = data.randomSplit([0.8, 0.2], seed=42)
# Create a Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="TreeDBH")
# Train the model
model = lr.fit(train_data_dbh)
# Make predictions on the testing data
predictions = model.transform(test_data_dbh)
# Evaluate the model's performance
evaluator = RegressionEvaluator(labelCol='TreeDBH', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions, {evaluator.metricName: 'mse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print("\n\n\n")
result.write("\n\n\n")
print("# ---------------------------------- DBH ---------------------------------- #")
print("Median Income VS. Tree DBH:\n")
result.write("Median Income VS. Tree DBH:\n")
print('Mean squared error: %.2f' % mse)
result.write('Mean squared error: %.2f\n' % mse)
print('Coefficient of determination (R-squared): %.2f' % r2)
result.write('Coefficient of determination (R-squared): %.2f\n' % r2)
print('Intercept: %.2f' % model.intercept)
result.write('Intercept: %.2f\n' % model.intercept)
print('Slope: %.2f' % model.coefficients[0])
result.write('Slope: %.2f\n' % model.coefficients[0])
print('R-squared: %.2f' % r2)
result.write('R-squared: %.2f\n' % r2)


# # Select the relevant columns
# df = final.select(['MedianIncome', 'Good'])
# # Split the data into training and testing sets
# (training_data, testing_data) = df.randomSplit([0.8, 0.2], seed=42)
# # Assemble the features into a vector
# assembler = VectorAssembler(inputCols=['MedianIncome'], outputCol='features')
# training_data = assembler.transform(training_data)
# testing_data = assembler.transform(testing_data)
# Select columns for input features and target variable
assembler = VectorAssembler(inputCols=["MedianIncome"], outputCol="features")
data = assembler.transform(final).select("Good", "features")
# Split data into training and testing sets
training_data_good, testing_data_good = data.randomSplit([0.8, 0.2], seed=42)
# Create a linear regression model
lr = LinearRegression(featuresCol='features', labelCol='Good')
# Train the model
model = lr.fit(training_data_good)
# Make predictions
predictions = model.transform(testing_data_good)
# Evaluate the model's performance
evaluator = RegressionEvaluator(labelCol='Good', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions, {evaluator.metricName: 'mse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print("\n\n\n")
result.write("\n\n\n")
print("# ---------------------------------- Good ---------------------------------- #")
print("Median Income VS. Health of Trees (Good):\n")
result.write("Median Income VS. Health of Trees (Good):\n")
print('Mean squared error: %.2f' % mse)
result.write('Mean squared error: %.2f\n' % mse)
print('Coefficient of determination (R-squared): %.2f' % r2)
result.write('Coefficient of determination (R-squared): %.2f\n' % r2)
print('Intercept: %.2f' % model.intercept)
result.write('Intercept: %.2f\n' % model.intercept)
print('Slope: %.2f' % model.coefficients[0])
result.write('Slope: %.2f\n' % model.coefficients[0])
print('R-squared: %.2f' % r2)
result.write('R-squared: %.2f\n' % r2)


# Select columns for input features and target variable
assembler = VectorAssembler(inputCols=["MedianIncome"], outputCol="features")
data = assembler.transform(final).select("Fair", "features")
# Split data into training and testing sets
training_data_fair, testing_data_fair = data.randomSplit([0.8, 0.2], seed=42)
# Create a linear regression model
lr = LinearRegression(featuresCol='features', labelCol='Fair')
# Train the model
model = lr.fit(training_data_fair)
# Make predictions
predictions = model.transform(testing_data_fair)
# Evaluate the model's performance
evaluator = RegressionEvaluator(labelCol='Fair', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions, {evaluator.metricName: 'mse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print("\n\n\n")
result.write("\n\n\n")
print("# ---------------------------------- Fair ---------------------------------- #")
print("Median Income VS. Health of Trees (Fair):\n")
result.write("Median Income VS. Health of Trees (Fair):\n")
print('Mean squared error: %.2f' % mse)
result.write('Mean squared error: %.2f\n' % mse)
print('Coefficient of determination (R-squared): %.2f' % r2)
result.write('Coefficient of determination (R-squared): %.2f\n' % r2)
print('Intercept: %.2f' % model.intercept)
result.write('Intercept: %.2f\n' % model.intercept)
print('Slope: %.2f' % model.coefficients[0])
result.write('Slope: %.2f\n' % model.coefficients[0])
print('R-squared: %.2f' % r2)
result.write('R-squared: %.2f\n' % r2)


# Select columns for input features and target variable
assembler = VectorAssembler(inputCols=["MedianIncome"], outputCol="features")
data = assembler.transform(final).select("Poor", "features")
# Split data into training and testing sets
training_data_poor, testing_data_poor = data.randomSplit([0.8, 0.2], seed=42)
# Create a linear regression model
lr = LinearRegression(featuresCol='features', labelCol='Poor')
# Train the model
model = lr.fit(training_data_poor)
# Make predictions
predictions = model.transform(testing_data_poor)
# Evaluate the model's performance
evaluator = RegressionEvaluator(labelCol='Poor', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions, {evaluator.metricName: 'mse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
print("\n\n\n")
result.write("\n\n\n")
print("# ---------------------------------- Poor ---------------------------------- #")
print("Median Income VS. Health of Trees (Poor):\n")
result.write("Median Income VS. Health of Trees (Poor):\n")
print('Mean squared error: %.2f' % mse)
result.write('Mean squared error: %.2f\n' % mse)
print('Coefficient of determination (R-squared): %.2f' % r2)
result.write('Coefficient of determination (R-squared): %.2f\n' % r2)
print('Intercept: %.2f' % model.intercept)
result.write('Intercept: %.2f\n' % model.intercept)
print('Slope: %.2f' % model.coefficients[0])
result.write('Slope: %.2f\n' % model.coefficients[0])
print('R-squared: %.2f' % r2)
result.write('R-squared: %.2f\n' % r2)


result.write("\n\n\nDecisionTree Regresser Analysis\n\n")


# # Create a vector assembler
# assembler = VectorAssembler(inputCols=['MedianIncome'], outputCol='features')
# # Transform the data using the vector assembler
# data = assembler.transform(final).select("TreeDBH", "features")
# # Split the data into training and testing sets
# (trainingData, testData) = df.randomSplit([0.8, 0.2], seed=42)
# Create a decision tree model
dt = DecisionTreeRegressor(maxDepth=15, minInstancesPerNode=10, seed=42, featuresCol="features", labelCol="TreeDBH")
# Train the model
model = dt.fit(train_data_dbh)
# Make predictions on the test data
predictions = model.transform(test_data_dbh)
# Evaluate the model using Mean Squared Error
evaluator = RegressionEvaluator(labelCol='TreeDBH', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions, {evaluator.metricName: 'mse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
# Print the Mean Squared Error
print("\n\n\n")
result.write("\n\n\n")
print("Median Income VS. Tree DBH:\n")
result.write("Median Income VS. Tree DBH:\n")
print('Mean Squared Error = %.2f' % mse)
result.write('Mean squared error: %.2f\n' % mse)
print('R-squared: %.2f' % r2)
result.write('R-squared: %.2f\n' % r2)


# # Create a vector assembler
# assembler = VectorAssembler(inputCols=['MedianIncome'], outputCol='features')
# # Transform the data using the vector assembler
# data = assembler.transform(final).select("Good", "features")
# # Split the data into training and testing sets
# (trainingData, testData) = df.randomSplit([0.8, 0.2], seed=42)
# Create a decision tree model
dt = DecisionTreeRegressor(maxDepth=15, minInstancesPerNode=23, seed=42, featuresCol="features", labelCol="Good")
# Train the model
model = dt.fit(training_data_good)
# Make predictions on the test data
predictions = model.transform(testing_data_good)
# Evaluate the model using Mean Squared Error
evaluator = RegressionEvaluator(labelCol='Good', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions, {evaluator.metricName: 'mse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
# Print the Mean Squared Error
print("\n\n\n")
result.write("\n\n\n")
print("Median Income VS. Health of Trees (Good):\n")
result.write("Median Income VS. Health of Trees (Good):\n")
print('Mean Squared Error = %.2f' % mse)
result.write('Mean squared error: %.2f\n' % mse)
print('R-squared: %.2f' % r2)
result.write('R-squared: %.2f\n' % r2)


# # Create a vector assembler
# assembler = VectorAssembler(inputCols=['MedianIncome'], outputCol='features')
# # Transform the data using the vector assembler
# data = assembler.transform(final).select("Fair", "features")
# # Split the data into training and testing sets
# (trainingData, testData) = df.randomSplit([0.8, 0.2], seed=42)
# Create a decision tree model
dt = DecisionTreeRegressor(maxDepth=15, minInstancesPerNode=22, seed=42, featuresCol="features", labelCol="Fair")
# Train the model
model = dt.fit(training_data_fair)
# Make predictions on the test data
predictions = model.transform(testing_data_fair)
# Evaluate the model using Mean Squared Error
evaluator = RegressionEvaluator(labelCol='Fair', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions, {evaluator.metricName: 'mse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
# Print the Mean Squared Error
print("\n\n\n")
result.write("\n\n\n")
print("Median Income VS. Health of Trees (Fair):\n")
result.write("Median Income VS. Health of Trees (Fair):\n")
print('Mean Squared Error = %.2f' % mse)
result.write('Mean squared error: %.2f\n' % mse)
print('R-squared: %.2f' % r2)
result.write('R-squared: %.2f\n' % r2)


# # Create a vector assembler
# assembler = VectorAssembler(inputCols=['MedianIncome'], outputCol='features')
# # Transform the data using the vector assembler
# data = assembler.transform(final).select("Poor", "features")
# # Split the data into training and testing sets
# (trainingData, testData) = df.randomSplit([0.8, 0.2], seed=42)
# Create a decision tree model
dt = DecisionTreeRegressor(maxDepth=15, minInstancesPerNode=22, seed=42, featuresCol="features", labelCol="Poor")
# Train the model
model = dt.fit(training_data_poor)
# Make predictions on the test data
predictions = model.transform(testing_data_poor)
# Evaluate the model using Mean Squared Error
evaluator = RegressionEvaluator(labelCol='Poor', predictionCol='prediction', metricName='mse')
mse = evaluator.evaluate(predictions, {evaluator.metricName: 'mse'})
r2 = evaluator.evaluate(predictions, {evaluator.metricName: 'r2'})
# Print the Mean Squared Error
print("\n\n\n")
result.write("\n\n\n")
print("Median Income VS. Health of Trees (Poor):\n")
result.write("Median Income VS. Health of Trees (Poor):\n")
print('Mean Squared Error = %.2f' % mse)
result.write('Mean squared error: %.2f\n' % mse)
print('R-squared: %.2f' % r2)
result.write('R-squared: %.2f\n' % r2)
print("\n\n\n")
result.write("\n\n\n")