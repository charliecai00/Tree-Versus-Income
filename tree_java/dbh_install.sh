rm *.class *.jar
hdfs dfs -rm -r hw8/dbh/output/

javac -classpath `yarn classpath` -d . DBHMapper.java
javac -classpath `yarn classpath` -d . DBHReducer.java
javac -classpath `yarn classpath`:. -d . DBH.java

jar -cvf DBH.jar *.class
hadoop jar DBH.jar DBH hw8/tree_cleaned hw8/dbh/output/

hdfs dfs -cat hw8/dbh/output/part-r-00000