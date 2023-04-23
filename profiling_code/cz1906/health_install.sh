rm *.class *.jar
hdfs dfs -rm -r hw8/health/output/
cp ~/opencsv-5.7.1.jar ~/hw8/health

javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . HealthMapper.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . HealthReducer.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath`:. -d . Health.java

jar -cvf Health.jar *.class
hadoop jar Health.jar Health hw8/tree_cleaned.csv hw8/health/output/

hdfs dfs -cat hw8/health/output/part-r-00000
