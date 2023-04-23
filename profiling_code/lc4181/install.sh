rm *class Income.jar
hdfs dfs -rm -r ./output/

javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . IncomeMapper1.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . IncomeMapper2.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . IncomeReducer.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath`:. -d . Income.java

jar -cvf Income.jar *.class
hadoop jar Income.jar Income med_income_clean.csv med_income_zipcodes.csv ./output/

hdfs dfs -cat ./output/part-r-00000