rm *class Combine.jar
hdfs dfs -rm -r hw9/output/

javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . CombineMapper1.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . CombineMapper2.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . CombineMapper3.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . CombineReducer.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath`:. -d . Combine.java

jar -cvf Combine.jar *.class
hadoop jar Combine.jar Combine hw9/dbh.csv hw9/health.csv hw9/income.csv hw9/output/

hdfs dfs -cat hw9/output/part-r-00000