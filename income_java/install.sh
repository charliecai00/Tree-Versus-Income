rm *.class *.jar
hdfs dfs -rm -r hw8/income/output/
cp ~/opencsv-5.7.1.jar ~/hw8/income

javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . IncomeMapper1.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . IncomeMapper2.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . IncomeReducer.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath`:. -d . Income.java

jar -cvf Income.jar *.class
hadoop jar Income.jar Income hw8/med_income_zipcodes.csv hw8/med_income.csv hw8/income/output/

hdfs dfs -cat hw8/income/output/part-r-00000