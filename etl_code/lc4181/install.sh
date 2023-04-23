rm *.class Clean.jar
hdfs dfs -rm -r ./output

javac -classpath opencsv-5.7.1.jar:`yarn classpath` -d . CleanMapper.java
javac -classpath opencsv-5.7.1.jar:`yarn classpath`:. -d . Clean.java

jar -cvf Clean.jar *.class
hadoop jar Clean.jar Clean tree_data.csv ./output

hdfs dfs -cat ./output/part-r-00000