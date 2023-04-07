rm *.class *.jar
hdfs dfs -rm -r hw8/health/output/

javac -classpath `yarn classpath` -d . HealthMapper.java
javac -classpath `yarn classpath` -d . HealthReducer.java
javac -classpath `yarn classpath`:. -d . Health.java

jar -cvf Health.jar *.class
hadoop jar Health.jar Health hw8/tree_cleaned hw8/health/output/

hdfs dfs -cat hw8/health/output/part-r-00000