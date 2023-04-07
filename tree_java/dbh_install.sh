rm *.class *.jar
hdfs dfs -rm -r hw8/dbh/output/
cp ~/opencsv-5.7.1.jar ~/hw8/dbh

javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . DBHMapper.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . DBHReducer.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath`:. -d . DBH.java

jar -cvf DBH.jar *.class
hadoop jar DBH.jar DBH hw8/tree_cleaned.csv hw8/dbh/output/

hdfs dfs -cat hw8/dbh/output/part-r-00000