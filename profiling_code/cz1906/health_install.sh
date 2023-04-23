rm *.class Health.jar
hdfs dfs -rm -r ./output/

javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . HealthMapper.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . HealthReducer.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath`:. -d . Health.java

jar -cvf Health.jar *.class
hadoop jar Health.jar Health tree_data_cleaned.csv ./output/

hdfs dfs -cat ./output/part-r-00000
